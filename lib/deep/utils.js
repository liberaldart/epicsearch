'use strict'
const debug = require('debug')('eps:deep/utils')
const updater = require('js-object-updater')
const async = require('async-q')
const Q = require('q')
const _ = require('lodash')

const relationships = (cache, entityType) => {
  return _.pick(
    cache.es.config.schema[entityType],
    (fieldConfig) => {
      return fieldConfig.isRelationship 
    }
  )
}


const getEntity = (cache, eId, type) => {
  var key = eId + type
  if (!type || !eId) {
    throw new Error('Id or type missing in getEntity: id ' + eId + ' type ' + type)
  }
  if (cache.get(key)) {
    return Q(cache.get(key))
  } else {
    return cache.es.get.collect({
      index: type + 's',
      id: eId,
      type: type
    })
    .then((entity) => {
      if (!entity._source) {
        throw new Error('Entity not found. Type and id: ' + type + ' & ' +  eId)
      } else {
        //Avoid race condition so check in cache before setting
        if (!cache.get(key)) {
          cache.setEntity(entity)
          return entity
        } else {
          return cache.get(key)
        }
      }
    })
    .catch((err) => {
      debug(JSON.stringify(err))
      throw err
    })
  }
}

/**
 *
 * @param {Object} cache
 * @param {Object} sourceEntity
 * @param {Array or String} path - composed of String edges array or . separated path
 * @return {Array} - list of leaf entities of the tree governed by the path from sourceEntity
 */
const getEntitiesAtPath = (cache, sourceEntity, path) => {
  if (!path.length) {
    return Q(sourceEntity)
  }

  if (_.isString(path)) {
    path = path.split('.')
  }
  const edge = _.first(path)
  return getEntitiesAtEdge(cache, sourceEntity, edge)
  .then((edgeEntities) => {

    let isNotArray = !_.isArray(cache.es.config.schema[sourceEntity._type][edge].type)

    if (!edgeEntities) {
      return isNotArray && Q() || Q([])
    }
    //Convert to array for iteration
    edgeEntities = isNotArray && [edgeEntities] || edgeEntities
    return async.map(edgeEntities, (edgeEntity) => {
      if (path.length > 1) {
        return getEntitiesAtPath(cache, edgeEntity, _.drop(path, 1))
      } else {
        return isNotArray && _.first(edgeEntities) || edgeEntities
      }
    })
    .then((tree) => {
      const entitiesAtPath = _(tree).flatten().uniq().compact().value()
      return entitiesAtPath
    })
  })
}

const getEntitiesAtEdge = (cache, sourceEntity, edge) => {
  let entitiesWithId = sourceEntity._source[edge]
  if (!entitiesWithId) {
    return Q()
  }
  let isNotArray = false
  if (!_.isArray(entitiesWithId)) {
    isNotArray = true
    entitiesWithId = [entitiesWithId]
  }
  const destEntityType = _.get(cache.es.config.schema, [sourceEntity._type, edge])
  return async.map(entitiesWithId, (entityWithId) => {
    return getEntity(cache, entityWithId._id, destEntityType.to)
  })
  .then((entities) => {
    entities = _.compact(entities)
    if (isNotArray) {
      return _.first(entities)
    } else {
      return entities
    }
  })
}


const fieldType = (config, entityType, field) => {
  var fieldSchema = config.schema[entityType][field]
  return _.flatten([fieldSchema.type])[0]
}

const updateCopyToSibling = (updatedEntity, relation, relatedEId, fieldUpdate, cache) => {
  return getEntity(cache, relatedEId, fieldType(cache.es.config, updatedEntity._type, relation))
  .then((relatedEntity) => {
    return cache.es.deep.update(
      {
        _id: relatedEntity._id,
        _type: relatedEntity._type,
        update: fieldUpdate
      },
      cache
    )
    .then((res) => {
      cache.markDirtyEntity(relatedEntity)
      return res
    })
  })
}

const recalculateUnionInSibling = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField, cache) => {

  if (!_.get(sourceEntityNew, ['_source', updatedField]) && !_.get(sourceEntityOld, ['_source', updatedField])) {
    return Q()
  }
  //debug('recalculateUnionInSibling', sourceEntityNew._type, sourceEntityNew._id, 'updated field ', updatedField, 'new value', sourceEntityNew._source[updatedField], 'old value', sourceEntityOld && sourceEntityOld._source[updatedField], 'to update entity', toUpdateDestEntity)
  //Initialize meta for updatedField if necessary
  toUpdateDestEntity._source.meta = toUpdateDestEntity._source.meta || {}
  toUpdateDestEntity._source.meta[updatedField] = toUpdateDestEntity._source.meta[updatedField] || {}

  handleIncrements(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  handleDecrements(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)
  
  return updateUnionsInDestEntity(cache, toUpdateDestEntity, destEntityField)
}

const updateUnionsInDestEntity = (cache, toUpdateDestEntity, destEntityField) => {

  //Now calculate new field value in toUpdateDestEntity through the meta desrciptor
  let newFieldValue = _.transform(toUpdateDestEntity._source.meta[destEntityField], (result, count, id) => {
    if (!_.isUndefined(count) && count > 0) {
      result.push({_id: id})
    }
  }, [])
  if (_.isEmpty(newFieldValue)) {
    delete toUpdateDestEntity._source[destEntityField]
    newFieldValue = undefined
  }
  //console.log('after increment', toUpdateDestEntity._source.meta, cache.get(toUpdateDestEntity._id + toUpdateDestEntity._type)._source.meta)
  //Update the related entity and its tree with the new field value
  return cache.es.deep.update(
    {
      _id: toUpdateDestEntity._id,
      _type: toUpdateDestEntity._type,
      update: {set: {[destEntityField]: newFieldValue}}
    },
    cache
  )
  .then((res) => {
    cache.markDirtyEntity(toUpdateDestEntity)
    return res
  })
}

const handleIncrements = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {
  //Increment the count in meta of added values to the sourceEntityOld
  let increments = _.flatten([sourceEntityNew._source[updatedField]])
  if (sourceEntityOld) {
    increments = _.difference(increments, _.flatten([sourceEntityOld._source[updatedField]]))
  }
  increments = _.compact(increments)
  for (let value of increments) {//Update related entity accordingly
    toUpdateDestEntity._source.meta[destEntityField][value._id] = toUpdateDestEntity._source.meta[destEntityField][value._id] || 0
    toUpdateDestEntity._source.meta[destEntityField][value._id] += 1
  }
}

const handleDecrements = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {

  if (sourceEntityOld) {
    //Decrement counts in meta for the removed values from the sourceEntityOld
    let decrements = _.difference(_.flatten([sourceEntityOld && sourceEntityOld._source[updatedField]]), _.flatten([sourceEntityNew._source[updatedField]]))
    decrements = _.compact(decrements)
    decrements.forEach((value) => {//Update related entity accordingly
      const referenceCounts = toUpdateDestEntity._source.meta[destEntityField]
      //Reduce count by 1, and if it has become zero remove this key
      if (! --referenceCounts[value._id]) {
        delete referenceCounts[value._id]
        if (_.isEmpty(referenceCounts)) {
          delete toUpdateDestEntity._source.meta[destEntityField]
        }
      }
    })
  }
}

const recalculateJoinsInDestEntity = (cache, sourceEntityOld, sourceEntityNew, updatedField, destEntity, destEntityField) => {
    
}


if (require.main === module) {
  const EpicSearch = require('../../index')
  const conf = process.argv[2]
  const es = new EpicSearch(conf)
  const Cache = require('../cache')
  const cache = new Cache(es)
  const ctx = {}
  /**es.dsl.execute('get event 1', ctx)
  .then((event) => {
    return getEntitiesAtPath(cache, ctx.event, 'sessions.video')
    .then(debug)
  })
  .catch(debug)**/

  es.dsl.execute('get session 1', ctx)
  .then(() => {
    debug(ctx.session, 'fgg')
    return getEntitiesAtPath(cache, ctx.session, 'event.speakers')
    .then((entities) => {
      debug(entities, cache.data)
    })
  })
  .catch(debug)
}
module.exports.relationships = relationships
module.exports.recalculateUnionInSibling = recalculateUnionInSibling
module.exports.updateCopyToSibling = updateCopyToSibling
module.exports.fieldType = fieldType
module.exports.getEntity = getEntity
module.exports.getEntitiesAtEdge = getEntitiesAtEdge
module.exports.getEntitiesAtPath = getEntitiesAtPath
