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

/**
 * @param {Array} edgePath - consisting of relationName followed by nodeIds. A relation will not exist by itself without _id of a node following it.
 * @return {Array} the path of just relations (without node ids)
 */
const extractRelationWisePath = (edgePath) => {
  const isEvenNumberEntry = (item, i) => {
    return i % 2 === 0
  }
  return _.filter(edgePath, isEvenNumberEntry)
}

const getEntity = (cache, eId, type, fields) => {
  var key = eId + type
  if (!type || !eId) {
    throw new Error('Id or type missing in getEntity: id ' + eId + ' type ' + type)
  }
  let existingEntity = cache.get(key)
  if (existingEntity) {
    return Q(existingEntity)
  } else {
    return cache.es.get.collect({
      index: type + 's',
      id: eId,
      fields: fields,
      type: type
    })
    .then((entity) => {
      if (!entity._source) {
        throw new Error('Entity not found. Type and id: ' + type + ' & ' +  eId)
      } else {
        //Avoid race condition so check in cache before setting
        existingEntity = cache.get(key)

        if (!existingEntity) {
          cache.setEntity(entity)
          //if (entity._type === 'session')
          //  debug('found entity in cache after es hit', entity)
          return entity

        } else {
          //if (existingEntity._type === 'session')
          //  debug('found entity in cache after es hit', existingEntity)
          return cache.get(key)
        }
      }
    })
  }
}

/**
 *
 * @param {Object} cache
 * @param {Object} sourceEntity
 * @param {Array or String} relationPath - composed of String seqyence of relations or . separated relation path
 * @param {[String]} edgePathToSource
 * @param {Object} idToEdgePathMap
 * @return {Object} - {entities: <list of leaf entities of the tree governed by the path from sourceEntity>, idToEdgePathMap: <map from id of entities to edge wise path from sourceEntity to that entity>}
 */
const getEntitiesAtRelationPath = (cache, sourceEntity, relationPath, edgePathToSource, idToEdgePathMap) => {

  idToEdgePathMap = idToEdgePathMap || {}

  if (!relationPath.length) {
    idToEdgePathMap[sourceEntity._id] = edgePathToSource || new Error().stack
    return Q({entities: sourceEntity, idToEdgePathMap: idToEdgePathMap})
  }

  if (_.isString(relationPath)) {
    relationPath = relationPath.split('.')
  }


  const firstRelation = _.first(relationPath)
  return getEntitiesInRelation(cache, sourceEntity, firstRelation)
  .then((relationEntities) => {


    let isNotArray = cache.es.config.schema[sourceEntity._type][firstRelation].cardinality === 'one'

    if (!relationEntities) {
      return isNotArray && Q({entities: undefined}) || Q({entities: []})
    }
    //Convert to array for iteration
    relationEntities = isNotArray && [relationEntities] || relationEntities
    return async.map(relationEntities, (relationEntity) => {
      const edgePathSoFar = (edgePathToSource || []).concat([firstRelation, relationEntity._id])
      if (relationPath.length > 1) {
        return getEntitiesAtRelationPath(cache, relationEntity, _.drop(relationPath, 1), edgePathSoFar, idToEdgePathMap)
      } else {
        //relatedEntity.edgePathFromSource = edgePathSoFar
        idToEdgePathMap[relationEntity._id] = edgePathSoFar
        return {entities: relationEntity}
      }
    })
    .then((result) => {
      const entitiesAtPath = _(result).map((r) => r.entities).flatten().uniq('_id').compact().value()
      return {
        entities: entitiesAtPath,
        idToEdgePathMap: idToEdgePathMap
      }
    })
  })
}
/**
 * Whether any of the path in paths contains the given path
 * @param {[String]} paths - List of paths
 * @param {String} path - path to check
 *
 */
const containsPath = (paths, path) => {
  if (!paths) {
    return false
  }
  for (const p of paths) {
    if (p.length != path.length) {
      return false
    }
    for (const i in p) {
      if (!_.isEqual(p[i], path[i])) {
        break 
      }
      if (i == (p.length - 1)) {
        return true
      }
    }
  }
  return false
} 

const getEntitiesInRelation = (cache, sourceEntity, relation) => {
  let entitiesWithId = (sourceEntity._source || sourceEntity.fields)[relation]
  if (!entitiesWithId) {
    return Q()
  }
  let isNotArray = false
  if (!_.isArray(entitiesWithId)) {
    isNotArray = true
    entitiesWithId = [entitiesWithId]
  }
  const destEntityType = _.get(cache.es.config.schema, [sourceEntity._type, relation])
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

/**
 * @param {Object} cache
 * @param {Object} startNode
 * @param [String] edgePath - [(relation, nodeId)+]
 */
const getEntityAtEdgePath = (cache, startNode, edgePath) => {
  const relatedNodeType = cache.es.config.schema[startNode._type][edgePath[0]].to
  let lastNode
  return getEntity(cache, edgePath[1], relatedNodeType)
  .then((nextNode) => {
    lastNode = nextNode
    if (edgePath.length == 2) {
      return lastNode
    }
    return getEntityAtEdgePath(cache, nextNode, _.drop(edgePath, 2))
  })
  
}


const recalculateUnionInSibling = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField, cache) => {
  if (!cache.es.config.schema[sourceEntityNew._type][updatedField].isRelationship) {
    return Q()
  }

  if (!_.get(sourceEntityNew, ['_source', updatedField]) && !_.get(sourceEntityOld, ['_source', updatedField])) {
    return Q()
  }
  //debug('recalculateUnionInSibling', sourceEntityNew._type, sourceEntityNew._id, 'updated field ', updatedField, 'new value', sourceEntityNew._source[updatedField], 'old value', sourceEntityOld && sourceEntityOld._source[updatedField], 'to update entity', JSON.stringify(toUpdateDestEntity))
  //Initialize meta for updatedField if necessary
  toUpdateDestEntity._source.meta = toUpdateDestEntity._source.meta || {}
  toUpdateDestEntity._source.meta[destEntityField] = toUpdateDestEntity._source.meta[destEntityField] || {}

  const newLinks = 
    handleIncrements(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  const removedLinks = 
    handleDecrements(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  return updateUnionsInDestEntity(cache, toUpdateDestEntity, destEntityField)
}

const handleIncrements = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {
  //Increment the count in meta of added values to the sourceEntityOld
  let newIds = _([(sourceEntityNew._source || sourceEntityNew.fields)[updatedField]])
    .flatten()
    .compact()
    .map('_id')
    .value()

  let increments = newIds

  if (sourceEntityOld) {
    let oldIds = _([(sourceEntityOld._source || sourceEntityOld.fields)[updatedField]])
      .flatten()
      .compact()
      .map('_id')
      .value()

    increments = _.difference(newIds, oldIds)
  }
  
  for (let eId of increments) {//Update related entity accordingly
    toUpdateDestEntity._source.meta[destEntityField][eId] = toUpdateDestEntity._source.meta[destEntityField][eId] || 0
    toUpdateDestEntity._source.meta[destEntityField][eId] += 1
  }
  return increments
}

const handleDecrements = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {

  if (sourceEntityOld) {
    let newIds = _([sourceEntityNew._source[updatedField]])
      .flatten()
      .compact()
      .map('_id')
      .value()
    let oldIds = _([sourceEntityOld && sourceEntityOld._source[updatedField]])
      .flatten()
      .compact()
      .map('_id')
      .value()
      //Decrement counts in meta for the removed values from the sourceEntityOld
    const decrements = _.difference(oldIds, newIds)
    //debug(sourceEntityNew._type, 'found decrements', decrements, sourceEntityNew._source[updatedField], sourceEntityOld._source[updatedField])

    decrements.forEach((id) => {//Update related entity accordingly
      const referenceCounts = toUpdateDestEntity._source.meta[destEntityField]
      //Reduce count by 1, and if it has become zero remove this key
      if (! --referenceCounts[id]) {
        delete referenceCounts[id]
        if (_.isEmpty(referenceCounts)) { //If no more references are left, delete the empty object too
          delete toUpdateDestEntity._source.meta[destEntityField]
        }
      }
    })
    return decrements
  }
}

const updateUnionsInDestEntity = (cache, toUpdateDestEntity, destEntityField, increments, decrements) => {

  const existingLinks = toUpdateDestEntity._source[destEntityField]

  //Now calculate new field value in toUpdateDestEntity through union of the reference counts and ownership
  const linksWithPositiveCounts = _.transform(toUpdateDestEntity._source.meta[destEntityField], (result, count, id) => {

    if (!_.isUndefined(count) && count > 0) {
      //Push joined object if it exists, or push just the _id, to be later joined
      result.push(_.find(existingLinks, {_id: id}) || {_id: id})
    }
  }, [])

  const ownLinks = _.filter(existingLinks, {own: true})
  let newFieldValue = 
    _(ownLinks)
    .union(linksWithPositiveCounts)
    .value()

  //For new links, resolveJoins and push
  //For removed links, call pull
  let updateInstruction
  if (_.isEmpty(newFieldValue)) {
    updateInstruction = {unset: {_path: destEntityField}}
  } else {
    updateInstruction = {set: {[destEntityField]: newFieldValue}} 
  }
  debug('about to deep update unionIn', JSON.stringify(toUpdateDestEntity), 'new field value', JSON.stringify(newFieldValue), ' for field', destEntityField, 'updateInstruction', updateInstruction)
  //Update the related entity and its tree with the new field value
  return cache.es.deep.update(
    {
      _id: toUpdateDestEntity._id,
      _type: toUpdateDestEntity._type,
      update: updateInstruction 
    },
    cache
  )
  .then((res) => {
    cache.markDirtyEntity(toUpdateDestEntity)
    return res
  })
}

/**
 * @param {Array} path - the path which is to be checked as super path of subPath
 * @parah {Array} subPath
 */
const startsWith = (path, subPath) => {
  let currentIndex = 0
  for (let relation of subPath) {
    if (path[currentIndex] != relation) {
      return false
    }
    ++currentIndex
  }
  return true
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
    return getEntitiesAtRelationPath(cache, ctx.session, 'event.speakers')
    .then((entities) => {
    })
  })
}
module.exports.relationships = relationships
module.exports.recalculateUnionInSibling = recalculateUnionInSibling
module.exports.getEntity = getEntity
module.exports.getEntitiesAtRelationPath = getEntitiesAtRelationPath
module.exports.extractRelationWisePath = extractRelationWisePath
module.exports.getEntityAtEdgePath = getEntityAtEdgePath
module.exports.containsPath = containsPath
module.exports.startsWith = startsWith
