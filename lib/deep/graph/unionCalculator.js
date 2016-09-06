const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph/unionCalculator')

const utils = require('../utils')
const traverser = require('./traverser')

module.exports.updateLeftNode = (cache, leftNode, edgePathToRightNode, rightNode) => {
  //debug('exploring possible update of node', leftNode._type, 'from (including its graph)', rightNode._type, 'with path', edgePathToRightNode)
  return updateLeftNodeWithRightNode(cache, leftNode, edgePathToRightNode, rightNode)
    .then(() => updateLeftNodeFromRightGraph(cache, leftNode, edgePathToRightNode, rightNode))
}

/**
 * if any of leftNode's content from thr rightNode through the node just before right node,
 * based on unionFrom, joinFrom type dependencies on the right graph
 */
const updateLeftNodeWithRightNode = (cache, leftNode, edgePathToRightNode, rightNode) => {
  if (edgePathToRightNode.length === 2) { //rightNode is immediate neighbor
    return Q()
  }
  //debug(leftNode, edgePathToRightNode, rightNode)
  
  const leftNodeSchema = cache.es.config.schema[leftNode._type]
  const relationPathToRightNode = utils.extractRelationWisePath(edgePathToRightNode)

  return async.each(_.keys(leftNodeSchema), (field) => {
    //Deal with only relationships in inboundUpdater 
    /**if (!leftNodeSchema[field].isRelationship) {
      return 
    }**/

    const fieldInfo = leftNodeSchema[field]
    const dependencyPaths = fieldInfo.unionFrom 
    if (!utils.containsPath(dependencyPaths, relationPathToRightNode)) {
      return
    }

    const edgePathToSecondLastNode = _.dropRight(edgePathToRightNode, 2)

    return utils.getEntityAtEdgePath(cache, leftNode, edgePathToSecondLastNode)
    .then((entityJustBeforeRightNode) => {
      const updateParams = {
        cache: cache,
        pathType: 'unionFrom',
        leftNode: leftNode,
        leftNodeFieldInfo: fieldInfo, 
        rightGraphNode: entityJustBeforeRightNode,
        rightGraphNodeOld: getOldValue(cache, entityJustBeforeRightNode, _.last(relationPathToRightNode), rightNode, true),
        relationPathToRightGraphNode: _.dropRight(relationPathToRightNode, 1),
        rightGraphNodeField: _.last(relationPathToRightNode)
      }
      //debug('updating field', updateParams.leftNodeFieldInfo.name, 'of', leftNode._type, 'from field', updateParams.rightGraphNodeField, 'of right node', updateParams.rightGraphNode._type, '. Path to right node', updateParams.relationPathToRightGraphNode)
      return doUnionFrom(updateParams).catch(debug)
    })
  })
}

/**
 * @param updateAction {Boolean} 
 */
const getOldValue = (cache, node, relation, entry, wasJustAdded) => {

  const relationSchema = cache.es.config.schema[node._type][relation]
  const latestEntries = (node._source || node.fields)[relation]
  let oldSource = _.cloneDeep(node._source || node.fields)

  if (wasJustAdded) {
    oldSource[relation] = relationSchema.cardinality === 'many' && _.filter(latestEntries, (e) => e._id != entry._id) || undefined
  } else { //was just removed
    oldSource[relation] = relationSchema.cardinality === 'many' && latestEntries.push(entry) || entry
  }
  
  const oldNode = _.omit(node, '_source')
  oldNode._source = oldSource
  return oldNode
}

const updateLeftNodeFromRightGraph = (cache, leftNode, edgePathToRightNode, rightNode) => {
  let promise = Q()
  if (leftNode._type === rightNode._type) { //Avoid cycles
    return promise
  }

  if (!leftNode._source && !leftNode.fields) {
    promise = promise.then(() => cache.es.get.collect({
      index: leftNode._type + 's',
      id: leftNode._id,
      type: leftNode._type
    }))
    .then((res) => leftNode = res)
  }
  if (!rightNode._source && !rightNode.fields) {
    promise = promise.then(() => cache.es.get.collect({
      index: rightNode._type + 's',
      id: rightNode._id,
      type: rightNode._type
    }))
    .then((res) => rightNode = res)
  }
  //debug('updating node', leftNode._type, 'from graph of', rightNode._type, 'with path to right node', utils.extractRelationWisePath(edgePathToRightNode))
  //Update this node based on each type of dependency
  return promise.then(() => traverser.traverseThroughRightNode(cache, 'unionFrom', leftNode, edgePathToRightNode, rightNode, doUnionFrom))
}

/**
 * Calculates union of leftNode's field based on updated values of relevant rightNode's field
@param {Object} cache
@param {String} pathType - For example joinFrom, unionFrom
@param {Object} leftNode
@param {Object} leftNodeFieldInfo - Info of the field for which doUnionFrom is to be done
@Param {Object} rightGraphNode - the source node for update of leftNode
@param {Object} rightGraphNodeOld - the older (before update) version of the source node where rightGraphNodeField also has older value
@param {String} rightGraphNodeField The field of right node under consideration for updating leftNodeFieldInfo.name field in leftNode
@param {[String]} relationPathToRightGraphNode - path from left node to right node in relations
@param {[String]} edgePathToRightNode
**/
const doUnionFrom = (params) => {
  return utils.loadEntities(params.cache, params.rightGraphNode, params.leftNode)
  .then((rightNodeLoaded, leftNode) => {

    return recalculateUnionInSibling(params.rightGraphNodeOld, params.rightGraphNode, params.rightGraphNodeField, params.leftNode, params.leftNodeFieldInfo.name, params.cache)
  })
}

const recalculateUnionInSibling = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField, cache) => {
  /**if (!cache.es.config.schema[sourceEntityNew._type][updatedField].isRelationship) {
    return Q()
  }**/

  if (!_.get(sourceEntityNew, ['_source', updatedField]) && !_.get(sourceEntityOld, ['_source', updatedField])) {
    return Q()
  }
  //debug('recalculateUnionInSibling', sourceEntityNew._type, sourceEntityNew._id, 'updated field ', updatedField, 'new value', sourceEntityNew._source[updatedField], 'old value', sourceEntityOld && sourceEntityOld._source[updatedField], 'to update entity', JSON.stringify(toUpdateDestEntity))
  //Initialize meta for updatedField if necessary
  const toUpdateDestEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields
  toUpdateDestEntityBody.meta = toUpdateDestEntityBody.meta || {}
  toUpdateDestEntityBody.meta[destEntityField] = toUpdateDestEntityBody.meta[destEntityField] || {}

  const newLinks = 
    handleIncrements(cache, sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  const removedLinks = 
    handleDecrements(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  return updateUnionsInDestEntity(cache, toUpdateDestEntity, destEntityField)
}

const handleIncrements = (cache, sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {

  let increments

  if (cache.es.config.schema[sourceEntityNew._type][updatedField].isRelationship) {
    increments = getIncrementsForRelation(sourceEntityOld, sourceEntityNew, updatedField)  
  } else {
  
    increments = getIncrementsForField(sourceEntityOld, sourceEntityNew, updatedField)  
  }
  
  const toUpdateDestEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields
    
 // debug('dddddddddddddddddddddddd', increments, sourceEntityNew, sourceEntityOld)
  for (let value of increments) {//Update related entity accordingly
    toUpdateDestEntityBody.meta[destEntityField][value] = toUpdateDestEntityBody.meta[destEntityField][value] || 0
    toUpdateDestEntityBody.meta[destEntityField][value] += 1
  }
  return increments
}

const getIncrementsForRelation = (sourceEntityOld, sourceEntityNew, updatedField) => {
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

  return increments

}

const getIncrementsForField = (sourceEntityOld, sourceEntityNew, updatedField) => {
  
  let newValues = _([(sourceEntityNew._source || sourceEntityNew.fields)[updatedField]])
    .flatten()
    .compact()
    .value()

  let increments = newValues

  if (sourceEntityOld) {
    let oldValues = _([(sourceEntityOld._source || sourceEntityOld.fields)[updatedField]])
      .flatten()
      .compact()
      .value()

    increments = _.difference(newValues, oldValues)
  }

  return increments
}


const handleDecrements = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {

  if (sourceEntityOld) {
    let newIds = _([(sourceEntityNew._source || sourceEntityNew.fields)[updatedField]])
      .flatten()
      .compact()
      .map('_id')
      .value()
    let oldIds = _([sourceEntityOld && (sourceEntityOld._source || sourceEntityOld.fields)[updatedField]])
      .flatten()
      .compact()
      .map('_id')
      .value()
      //Decrement counts in meta for the removed values from the sourceEntityOld
    const decrements = _.difference(oldIds, newIds)
    //debug(sourceEntityNew._type, 'found decrements', decrements, sourceEntityNew._source[updatedField], sourceEntityOld._source[updatedField])
    
    const toUpdateDestEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields

    decrements.forEach((id) => {//Update related entity accordingly
      const referenceCounts = toUpdateDestEntityBody.meta[destEntityField]
      //Reduce count by 1, and if it has become zero remove this key
      if (! --referenceCounts[id]) {
        delete referenceCounts[id]
        if (_.isEmpty(referenceCounts)) { //If no more references are left, delete the empty object too
          delete toUpdateDestEntityBody.meta[destEntityField]
        }
      }
    })
    return decrements
  }
}

const updateUnionsInDestEntity = (cache, toUpdateDestEntity, destEntityField, increments, decrements) => {

  const toUpdateDestEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields
  const existingValues = toUpdateDestEntityBody[destEntityField]
  const isRelation = cache.es.config.schema[toUpdateDestEntity._type][destEntityField].isRelationship

  //Now calculate new field value in toUpdateDestEntity through union of the reference counts and ownership
  const valuesWithPositiveCounts = _.transform(toUpdateDestEntityBody.meta[destEntityField], (result, count, value) => {

    if (!_.isUndefined(count) && count > 0) {
      //Push joined object if it exists, or push just the _id, to be later joined
      if (isRelation) {
        result.push(_.find(existingValues, {_id: value}) || {_id: value})
      } else {
        result.push(value)
      }
    }
  }, [])

  let newFieldValue = 
    _(isRelation && _.filter(existingValues, {own: true}) || undefined)
    .union(valuesWithPositiveCounts)
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
 * The sourceEntity has been updated to sourceEntityNew by applying an update on a field. Update the nodes connected to sourceEntity and having fields which unionFrom this particular field of sourceEntity. 
 * @param {Object} cache
 * @param {Object} sourceEntity
 * @param {Object} sourceEntityNew
 * @param {String} updatedField
 */
module.exports.recalculateUnionInDependentGraph = (cache, sourceEntityOld, sourceEntityNew, updatedField) => {
  const entitySchema = cache.es.config.schema[sourceEntityNew._type]
  const updatedFieldSchema = entitySchema[updatedField]
  const unionInPaths = _([updatedFieldSchema.unionIn]).flatten().compact().value()

  return async.each(unionInPaths, (unionInPath) => {
    return utils.getEntitiesAtRelationPath(cache, sourceEntityNew, _.dropRight(unionInPath, 1))
    .then((toUpdateDestEntities) => {
      if (!toUpdateDestEntities.entities) {
        return
      }
      if (_.isArray(toUpdateDestEntities.entities)) {
        if (!_.size(_.compact(toUpdateDestEntities.entities))) {
          return
        }
      } else { //Make it an array
        toUpdateDestEntities.entities = [toUpdateDestEntities.entities]
      }
      //debug('recalculateUnionInGraph: will update union at path', unionInPath, 'sourceEntityNew', sourceEntityNew._source, 'sourceEntityOld._source', sourceEntityOld, 'to update entities', JSON.stringify(toUpdateDestEntities))
      return async.each(toUpdateDestEntities.entities, (toUpdateDestEntity) => {
        const destFieldToUpdate = _.last(unionInPath)
        return recalculateUnionInSibling(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destFieldToUpdate, cache)
      })
      //.then(() => {
      //  debug(updatedField, sourceEntityNew)
      //})
    })
  })
}

