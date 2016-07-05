'use strict'
const debug = require('debug')('eps:deep/utils')
const updater = require('js-object-updater')
const async = require('async-q')
const Q = require('q')
const _ = require('lodash')

const getEntity = function(cache, eId, type) {
  var key = eId + type
  if (!type || !eId) {
    throw new Error('Id or type missing in getEntity: ' + eId + ' type ' + type)
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
        cache.setEntity(entity)
        return entity
      }
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
  if (_.isString(path)) {
    path = path.split('.')
  }
  const edge = _.first(path)
  return getEntitiesAtEdge(cache, sourceEntity, edge)
  .then((edgeEntities) => {
    let isNotArray = false
    if (!_.isArray(edgeEntities)) {
      edgeEntities = [edgeEntities]
      isNotArray = true
    }
    return async.map(edgeEntities, (edgeEntity) => {
      if (path.length > 1) {
        return getEntitiesAtPath(cache, edgeEntity, _.drop(path, 1))
      } else {
        return isNotArray && _.first(edgeEntities) || edgeEntities
      }
    })
    .then((tree) => {
      return _(tree).flatten().uniq().value()
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

//  debug('recalculateUnionInSibling', sourceEntityNew._type, sourceEntityNew._id, 'updated field ', updatedField, 'new value', sourceEntityNew._source[updatedField], 'old value', sourceEntityOld && sourceEntityOld._source[updatedField], 'to update entity', _.pick(toUpdateDestEntity, ['_id', '_type']))

  //Initialize meta for updatedField if necessary
  toUpdateDestEntity._source.meta = toUpdateDestEntity._source.meta || {}
  toUpdateDestEntity._source.meta[updatedField] = toUpdateDestEntity._source.meta[updatedField] || {}

  handleDecrements(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  handleIncrements(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  //Now calculate new field value in toUpdateDestEntity through the meta desrciptor
  let newFieldValue = _.transform(toUpdateDestEntity._source.meta[destEntityField], (result, value, key) => {
    if (!_.isUndefined(value) && value > 0) {
      result.push(key)
    }
  }, [])
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
    increments = _.difference(increments, _.flatten([sourceEntityOld._source[udpatedField]]))
  }
  increments = _.compact(increments)
  increments.forEach((value) => {//Update related entity accordingly
    toUpdateDestEntity._source.meta[destEntityField][value] = toUpdateDestEntity._source.meta[destEntityField][value] || 0
    toUpdateDestEntity._source.meta[destEntityField][value] += 1
  })
  //debug('increments', increments, toUpdateDestEntity._source[destEntityField], toUpdateDestEntity._source.meta)

}

const handleDecrements = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {

  if (sourceEntityOld) {
    //Decrement counts in meta for the removed values from the sourceEntityOld
    let decrements = _.difference(_.flatten([sourceEntityOld && sourceEntityOld._source[updatedField]]), _.flatten([sourceEntityNew._source[updatedField]]))
    decrements = _.compact(decrements)
    decrements.forEach((value) => {//Update related entity accordingly
      toUpdateDestEntity._source.meta[destEntityField][value] -= 1
    })
    //debug('decrements', decrements, sourceEntityNew._source[updatedField], toUpdateDestEntity._source.meta)
  }
}

if (require.main === module) {
  const EpicSearch = require('../../index')
  const conf = process.argv[2]
  const es = new EpicSearch(conf)
  const Cache = require('../cache')
  const cache = new Cache(es)
  const ctx = {}
  es.dsl.execute('get event 1', ctx)
  .then((event) => {
    return getEntitiesAtPath(cache, ctx.event, 'sessions.video')
    .then(debug)
  })
  .catch(debug)

  es.dsl.execute('get session 1', ctx)
  .then((event) => {
    return getEntitiesAtPath(cache, ctx.session, 'event')
    .then(debug)
  })
  .catch(debug)
}
module.exports.recalculateUnionInSibling = recalculateUnionInSibling
module.exports.updateCopyToSibling = updateCopyToSibling
module.exports.fieldType = fieldType
module.exports.getEntity = getEntity
