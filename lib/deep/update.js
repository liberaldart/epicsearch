var debug = require('debug')('update')
var updater = require('js-object-updater')
var async = require('async-q')
var Q = require('q')
var _ = require('lodash')
// var es = require('../es')

function index(es) {
  this.es = es
}


/**
 *@param {entities: [], update:{}} for multiple entities update or {_id: '', _type: '', update: {}} for single entity
 */
index.prototype.execute  = (params, cachedEntities) => {
  var indexEntitiesAtEnd = false
  if (!cachedEntities) {
    cachedEntities = {}
    indexEntitiesAtEnd = true
  }
  params.entities = params.entities || [{_id: params._id, _type: params._type}]
  return async.each(params.entities, function(e) {
    return updateEntityAndSiblings(
      {
        _id: e._id,
        _type: e._type,
        update: params.update,
      },
      cachedEntities
    )
  })
  .then(() => {
    if (indexEntitiesAtEnd) {
      return async.each(_.values(cachedEntities), (updatedEntity) => {
        return es.index.collect({
          index: updatedEntity._index,
          type: updatedEntity._type,
          id: updatedEntity._id,
          body: updatedEntity._source
        })
      })
      .then((updates) => {
        cachedEntities.justUpdated = {
          _id: params._id,
          _type: params._type
        }
        return cachedEntities
      })
    }
  })
}

const updateEntityAndSiblings = (params, cachedEntities) => {
  params._type = params.type || params._type
  params._id = params._id || params.id

  return getEntity(cachedEntities, params._id, params._type)
  .then((entity) => {
    var updatedEntity = _.cloneDeep(entity)
    updater({
      doc: updatedEntity._source,
      update: params.update,
      force: true
    })
    cachedEntities[updatedEntity._id + updatedEntity._type] = updatedEntity
    //debug('updateEntityAndSiblings', JSON.stringify(params))
    //debug('just set in cachedEntities', updatedEntity._source)
    return doAllUpdates(entity, updatedEntity, params, cachedEntities)
  })
}

const doAllUpdates = function(entity, updatedEntity, params, cachedEntities) {
  const entitySchema = this.es.config.schema[updatedEntity._type]
  return async.each(_.keys(entitySchema), (field) => {
    var fieldUpdate = pluckUpdatesForField(field, params.update)
    if (entity && _.isEmpty(fieldUpdate)) {//If this field is being updated, and updatedEntity represents a new entity being created
      return Q()
    }
    //debug('percolate Updates', updatedEntity._type, updatedEntity._id, 'field', field, 'update', fieldUpdate)

    var dif
    if (entity) {
      dif = _.difference(_.flatten([updatedEntity._source[field]]), _.flatten([entity._source[field]])).length
    }
    var fieldSchema = entitySchema[field]
    var updatePromises = []

    if (dif && fieldSchema.copyTo) {
      var copyToRelations = _.flatten([fieldSchema.copyTo])
      updatePromises.push(
        updateCopyToSiblings(updatedEntity, copyToRelations, fieldUpdate, cachedEntities))
    }

    if (fieldSchema.unionIn) {
      updatePromises.push(
        recalculateUnionInSiblings(entity, updatedEntity, field, cachedEntities))
    }

    if (fieldSchema.isRelationship) {
      updatePromises.push(handleNewLinkages(updatedEntity, field, fieldUpdate, cachedEntities))
    }

    return Q.all(updatePromises)

  })
}

const handleNewLinkages = (updatedEntity, field, fieldUpdate, cachedEntities) => {
  return async.each(['addToSet', 'push', 'set'], (updateType) => {
    if (fieldUpdate[updateType]) {

      var e2Type = fieldType(updatedEntity._type, field)
      var values = [fieldUpdate[updateType][field]]
      //debug('handleNewLinkages values', values)
      var e2Entities =
        _(values)
        .flatten()
        .map((e2Id) => {
          return {
            _type: e2Type,
            _id: e2Id
          }
        })
        .value()
      debug('handle possibly new linkages for type', updatedEntity._type, 'and _id', updatedEntity._id, 'with related entities of type', _.map(e2Entities, '_type'), field)
      return require('./link')({
          e1: updatedEntity,
          e1ToE2Relation: field,
          e2Entities: e2Entities
        },
        cachedEntities
      ).catch((err) => {
        debug('error in linking', err, updatedEntity, field, fieldUpdate, e2Entities)
      })
    } else {
      return Q()
    }
  })
}

const recalculateUnionInSiblings = function(entity, updatedEntity, field, cachedEntities) {
  var entitySchema = this.es.config.schema[updatedEntity._type]
  var fieldSchema = entitySchema[field]
  var unionInRelations = _.flatten([fieldSchema.unionIn])
  debug('recalculateUnionInSiblings: will calculate union on relations', unionInRelations, 'updatedEntity', updatedEntity._id, updatedEntity._type)
  return async.each(unionInRelations, (relation) => {
    var toUpdateEntityIds = updatedEntity._source[relation]
    //debug(updatedEntity._id, updatedEntity._type, relation, toUpdateEntityIds)
    if (!toUpdateEntityIds) {
      return Q()
    }
    //debug('recalculateUnionInSiblings', updatedEntity._type, updatedEntity._id, 'updated field ', field, 'relation', relation, 'relatedEIds', toUpdateEntityIds)
    if (!_.isArray(toUpdateEntityIds)) {
      toUpdateEntityIds = [toUpdateEntityIds]
    }
    return async.each(toUpdateEntityIds, (relatedEId) => {
      return recalculateUnionInSibling(field, entity, updatedEntity, relation, relatedEId, cachedEntities)
    })
  })
}

const recalculateUnionInSibling = (field, entity, updatedEntity, relation, relatedEId, cachedEntities) => {

  var relatedEntityType = fieldType(updatedEntity._type, relation)
  debug('recalculateUnionInSibling', updatedEntity._type, updatedEntity._id, 'updated field ', field, 'new value', updatedEntity._source[field], 'old value', entity && entity._source[field], 'relation', relation, 'related entity type', relatedEntityType, 'relatedEId', relatedEId)

  return getEntity(cachedEntities, relatedEId, relatedEntityType)//Load the entity in cache
  .then(() => {
    const relatedEntity = cachedEntities[relatedEId + relatedEntityType]
    //Initialize meta for field if necessary
    relatedEntity._source.meta = relatedEntity._source.meta || {}
    relatedEntity._source.meta[field] = relatedEntity._source.meta[field] || {}

    if (entity) {
      //Decrement counts in meta for the removed values from the entity
      var decrements = _.difference(_.flatten([entity && entity._source[field]]), _.flatten([updatedEntity._source[field]]))
      decrements = _.compact(decrements)
      decrements.forEach((value) => {//Update related entity accordingly
        relatedEntity._source.meta[field][value] -= 1
      })
      debug('decrements', decrements, updatedEntity._source[field], relatedEntity._source.meta)
    }
    //Increment the count in meta of added values to the entity
    var increments = _.flatten([updatedEntity._source[field]])
    if (entity) {
      increments = _.difference(increments, _.flatten([entity._source[field]]))
    }
    increments = _.compact(increments)
    increments.forEach((value) => {//Update related entity accordingly
      relatedEntity._source.meta[field][value] = relatedEntity._source.meta[field][value] || 0
      relatedEntity._source.meta[field][value] += 1
    })
    debug('increments', increments, relatedEntity._source[field], relatedEntity._source.meta)

    //Now calculate new field value in related entity through the meta desrciptor
    var newFieldValue = _.transform(relatedEntity._source.meta[field], (result, value, key) => {
      if (!_.isUndefined(value) && value > 0) {
        result.push(key)
      }
    }, [])
    //Update the related entity and its tree with the new field value
    return module.exports(
      {
        _id: relatedEntity._id,
        _type: relatedEntity._type,
        update: {set: {[field]: newFieldValue}}
      },
      cachedEntities
    )
    .catch((err) => {
      debug('Err in recursive update', err)
    })
  })
}

const updateCopyToSiblings = (updatedEntity, relations, fieldUpdate, cachedEntities) => {

  return async.each(relations, (relation) => {
    var toUpdateEntityIds = updatedEntity._source[relation]
    if (!toUpdateEntityIds) {
      return Q()
    }
    if (!_.isArray(toUpdateEntityIds)) {
      toUpdateEntityIds = [toUpdateEntityIds]
    }
    return async.each(toUpdateEntityIds, (relatedEId) => {
      return updateCopyToSibling(updatedEntity, relation, relatedEId, fieldUpdate, cachedEntities)
    })
  })
}

const updateCopyToSibling = (updatedEntity, relation, relatedEId, fieldUpdate, cachedEntities) => {
  return getEntity(cachedEntities, relatedEId, fieldType(updatedEntity._type, relation))
  .then((relatedEntity) => {
    return module.exports(
      {
        _id: relatedEntity._id,
        type: relatedEntity._type,
        update: fieldUpdate
      },
      cachedEntities
    )
  })
}

const getEntity = (entityCache, eId, type) => {
  var key = eId + type
  if (!type || !eId) {
    throw new Error('Id or type missing in getEntity: ' + eId + ' type ' + type)
  }
  if (entityCache[key]) {
    return Q(entityCache[key])
  } else {
    return es.get.collect({
      index: type + 's',
      id: eId,
      type: type
    })
    .then((entity) => {
      if (!entity._source) {
        throw new Error('Entity not found. Type and id: ' + type + ' & ' +  eId)
      } else {
        entityCache[key] = entity
        return entity
      }
    })
  }
}

const fieldType = function(entityType, field) {
  var fieldSchema = this.es.config.schema[entityType][field]
  return _.flatten([fieldSchema.type])[0]
}

const pluckUpdatesForField = (field, update) => {
  return _.transform(update, (result, updateTypeInstructions, updateType) => {
    if (!_.isArray(updateTypeInstructions)) {
      updateTypeInstructions = [updateTypeInstructions]
    }
    updateTypeInstructions.forEach((instruction) => {
      if (instruction[field]) {
        result[updateType] = result[updateType] || {}
        result[updateType][field] = instruction[field]
      }
    })
  }, {})
}

index.doAllUpdates = doAllUpdates
index.recalculateUnionInSibling = recalculateUnionInSibling
index.updateCopyToSibling = updateCopyToSibling
index.getEntity = getEntity
module.exports = index

if (require.main === module) {
  module.exports({"entities":[{"_id":"hd--Untitled--Event -3719/4741-2015-06-21-himachal-thekchen-choling-temple-ceremony","_type":"folder"}],"update":{"set":{"session":"AVOoOLCybOcXv7EUyIUZ"}},"context":"web.read","lang":"english"})
  .then(function(res) {
    debug('Updated', JSON.stringify(res))
  })
  .catch(debug)
}
