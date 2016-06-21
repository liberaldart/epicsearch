'use strict'
const debug = require('debug')('update')
const updater = require('js-object-updater')
const async = require('async-q')
const Q = require('q')
const _ = require('lodash')

const utils = require('./utils')
// var es = require('../es')

function index(es) {
  this.es = es
}


/**
 *@param {entities: [], update:{}} for multiple entities update or {_id: '', _type: '', update: {}} for single entity
 */
index.prototype.execute  = function (params, cachedEntities) {
  var indexEntitiesAtEnd = false
  if (!cachedEntities) {
    cachedEntities = {}
    indexEntitiesAtEnd = true
  }
  params.entities = params.entities || [{_id: params._id, _type: params._type}]
  const index = this
  return async.each(params.entities, function(e) {
    return index.updateEntityAndSiblings(
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

index.prototype.updateEntityAndSiblings = function (params, cachedEntities) {
  params._type = params.type || params._type
  params._id = params._id || params.id
  const index = this
  return utils.getEntity(cachedEntities, params._id, params._type, index.es)
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
    return index.doAllUpdates(entity, updatedEntity, params, cachedEntities)
  })
}

index.prototype.doAllUpdates = function(entity, updatedEntity, params, cachedEntities) {
  const entitySchema = this.es.config.schema[updatedEntity._type]
  const index = this
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
        index.updateCopyToSiblings(updatedEntity, copyToRelations, fieldUpdate, cachedEntities))
    }

    if (fieldSchema.unionIn) {
      updatePromises.push(
        utils.recalculateUnionInSiblings(entity, updatedEntity, field, cachedEntities, index.es))
    }

    if (fieldSchema.isRelationship) {
      updatePromises.push(
        index.handleNewLinkages(updatedEntity, field, fieldUpdate, cachedEntities))
    }

    return Q.all(updatePromises)

  })
}

index.prototype.handleNewLinkages = function (updatedEntity, field, fieldUpdate, cachedEntities) {
  return async.each(['addToSet', 'push', 'set'], (updateType) => {
    if (fieldUpdate[updateType]) {

      var e2Type = fieldType(this.es.config, updatedEntity._type, field)
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
      return this.es.deep.link({
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

index.prototype.recalculateUnionInSiblings = function(entity, updatedEntity, field, cachedEntities) {
  var entitySchema = this.es.config.schema[updatedEntity._type]
  var fieldSchema = entitySchema[field]
  var unionInRelations = _.flatten([fieldSchema.unionIn])
  const index = this
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
      return utils.recalculateUnionInSibling(field, entity, updatedEntity, relation, relatedEId, cachedEntities, index.es)
    })
  })
}

index.prototype.updateCopyToSiblings = function (updatedEntity, relations, fieldUpdate, cachedEntities) {

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

index.prototype.updateCopyToSibling = function (updatedEntity, relation, relatedEId, fieldUpdate, cachedEntities) {
  const index = this
  return utils.getEntity(cachedEntities, relatedEId, fieldType(index.es.config, updatedEntity._type, relation, index.es))
  .then((relatedEntity) => {
    return index(
      {
        _id: relatedEntity._id,
        type: relatedEntity._type,
        update: fieldUpdate
      },
      cachedEntities
    )
  })
}

const fieldType = function(config, entityType, field) {
  var fieldSchema = config.schema[entityType][field]
  return _.flatten([fieldSchema.type])[0]
}

const pluckUpdatesForField = function (field, update) {
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

index.prototype.execute.udpateCopyToSibling = index.prototype.udpateCopyToSibling
module.exports = index

if (require.main === module) {
  module.exports({"entities":[{"_id":"hd--Untitled--Event -3719/4741-2015-06-21-himachal-thekchen-choling-temple-ceremony","_type":"folder"}],"update":{"set":{"session":"AVOoOLCybOcXv7EUyIUZ"}},"context":"web.read","lang":"english"})
  .then(function(res) {
    debug('Updated', JSON.stringify(res))
  })
  .catch(debug)
}
