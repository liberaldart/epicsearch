'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph')

const utils = require('./utils')

/**
 * Updates the leftNode and the nodes linked to it (further down upto traversal depth), based on the dependency paths from those nodes going through the newly established leftNode->rightNode path
 * @param {Object} cache
 * @param {Object} leftNode
 * @param {Array | String} path => [(relationName, nodeId)+] From left node to right node 
 * @param {Object} rightNode
 *
 */
const updateLeftSubgraph = (cache, leftNode, path, rightNode, traversalDepth) => {
  if (!traversalDepth) {
    return Q()
  }
  return utils.getEntity(cache, leftNode._id, leftNode._type)
  .then((leftNode) => {
    return updateLeftNode(cache, leftNode, path, rightNode)
    .then(() => {

      //For every relation definition of left node
      const leftNodeRelations = utils.relationships(cache, leftNode._type)
      return async.each(_.keys(leftNodeRelations), (relationName) => {
        const relatedNodeInfo = cache.es.config.schema[leftNode._type][relationName]

        //For every related node connected to left node under that relationship definition
        //Get related Nodes
        let relatedNodes = (leftNode._source || leftNode.fields)[relationName]
        if (!relatedNodes) {
          return Q()
        }
        
        if (!_.isArray(relatedNodes)) {
          relatedNodes = [relatedNodes]
        }

        relatedNodes = relatedNodes.map((node) => {return {_id: node._id, _type: relatedNodeInfo.to}})

        //For each such node
        return async.each(relatedNodes, (relatedNode) => {

          //Update the related node and it's subgraph
          return updateLeftSubgraph(cache, relatedNode, [relatedNodeInfo.inName, leftNode._id].concat(path), rightNode, traversalDepth - 1) 
        })
      })
    })
  })
}

const INFORMATION_DEPENDENCY_TYPES = ['unionFrom']
const updateLeftNode = (cache, leftNode, edgePathToRightNode, rightNode) => {
  const leftNodeSchema = cache.es.config.schema[leftNode._type]
  //For every field of the leftNode - relations and properties
  return async.each(_.values(leftNodeSchema), (leftNodeFieldInfo) => {
   //Update this field based on each type of dependency
   return async.each(INFORMATION_DEPENDENCY_TYPES, (dependencyType) => {
      return updateFieldFromRightGraph(cache, dependencyType, leftNode, leftNodeFieldInfo, edgePathToRightNode, rightNode)
    })
  })
}


//Update this field based on each type of dependency. Different paths lead to the update dependencies on same field.
/**
 * @param {Object} cache
 * @param {String} dependencyType either of unionFrom, copyFrom, joinFrom
 * @param {Object} leftNodeFieldInfo
 * @param {Object} leftNode The node whose field (of which is the fieldInfo) is to be udpated based on another field in rightNode (*if applicable)
 * @param {Array} edgePathToRightNode Path consisting of relationship, _id pairs, leading from leftNode to rightNode.
 * @param {Object} rightNode the source of information from which left Node's field is to be updated (if applicable)
 *
 */
const updateFieldFromRightGraph = (cache, dependencyType, leftNode,leftNodeFieldInfo, edgePathToRightNode, rightNode) => {
  //Which paths to fetch depdendent field information from?
  const relationWisePathsForDependency = leftNodeFieldInfo[dependencyType] //Like unionFrom: ['session','speakers']
  if (!relationWisePathsForDependency) {
    return Q()
  }

  //Get relation wise path from edge path. Ex. sessions.speakers from sessions, 'session_id', speakers, 'speaker_id'
  const relationWisePathToRightNode = extractRelationWisePath(edgePathToRightNode)

  //Extract dependency relation paths emanating from this node which start with the relation wise path to right node
      //We traverse the new paths exposed by new connection of leftGraph with rightNode and its graph. So we start to walk from rightNode.
  ////Cut out the path ahead of rightNode, and from that remove the last edge of the path. The last edge is the field of leaf nodes from which the update is to be done in leftNode's field 
  const relationWisePathsAheadOfRightNode = 
    relationWisePathsForDependency
      .filter((path) => startsWith(path, relationWisePathToRightNode))
      .map((path) => _.drop(path, relationWisePathToRightNode.length))

  return udpateFieldFromPathsThroughRightNode(cache, dependencyType, leftNode, leftNodeFieldInfo, edgePathToRightNode, rightNode, relationWisePathsAheadOfRightNode)
}

const udpateFieldFromPathsThroughRightNode = (cache, dependencyType, leftNode, leftNodeFieldInfo, edgePathToRightNode, rightNode, relationPathsAheadOfRightNode) => {

  //For each of matched paths
  return async.each(relationPathsAheadOfRightNode, (relationPathAheadOfRightNode) => {

    //Get leaf entities at of that path when exploring from right node.
    return utils.getEntitiesAtPath(cache, rightNode, _.dropRight(relationPathAheadOfRightNode, 1))
    .then((nodesFromRightGraph) => {

      //The right node's field to udpate from
      //Can be a simple field or a relationship field. Doesn't matter
      const rightNodeField = _.last(relationPathAheadOfRightNode)

      if (!_.isArray(nodesFromRightGraph)) {
        nodesFromRightGraph = [nodesFromRightGraph] 
      }

      //RIght now only unionFrom is a dependeny. 
      //If in future there are dependencies like sumFrom, etc then their handling can be added as a switch case on dependency type
      return async.each(nodesFromRightGraph, (rightNode) => {
        return utils.recalculateUnionInSibling(null, rightNode, rightNodeField, leftNode, leftNodeFieldInfo.name, cache)
      })
    })
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

/**
 * @param {Array} edgePath - consisting of relationName followed by nodeIds. A relation will not exist by itself without _id of a node following it.
 * @return {Array} the path of just relations (without node ids)
 */
const extractRelationWisePath = (edgePath) => {
  return _.filter(edgePath, (item, i) => {
    return i % 2 === 0
  })
}

const recalculateJoinsInDependentGraph = (cache, updatedEntity, updatedField, fieldUpdate) => {
  const invertedJoinEntityConfig = cache.es.config.invertedJoins.index[updatedEntity._type] //index is the default config used for index time joins in graph
  const updatedFieldJoinIns = invertedJoinEntityConfig[field] //Inverted join info is stored as an array per field
  if (!updatedFieldJoinIns) {
    return
  }
  const joinInPaths = _.isArray(updatedFieldJoins) && updatedFieldJoins || [updatedFieldJoinIns]

  return async.each(joinInPaths, (joinInPath) => {
    return utils.getEntitiesAtPath(cache, updatedEntity, _.dropRight(joinInPath, 1))
    .then((toUpdateDestEntities) => {
      if (!toUpdateDestEntities || _.isEmpty(toUpdateDestEntities)) {
        return
      }
      if (!_.isArray(toUpdateDestEntities)) {
        toUpdateDestEntities = [toUpdateDestEntities]
      }
      //debug('recalculateUnionInGraph: will update union at path', unionInPath, 'sourceEntityNew', sourceEntityNew._source, 'sourceEntityOld._source', sourceEntityOld, 'to update entities', JSON.stringify(toUpdateDestEntities))
      return async.each(toUpdateDestEntities, (toUpdateDestEntity) => {
        const destRelationName = _.last(joinInPath)
        const destEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields
        const joinedDoc = _.find(destEntityBody[destRelationName], {_id: toUpdateDestEntity._id})

        if (!joinedDoc) {
          throw new Error('did not find joined doc. Updated entity', updatedEntity, 'updated field', updatedField, 'joinInPath', joinInPath, 'toUpdateDestEntity', toUpdateDestEntity)
        }
        updater({
          doc: joinedDoc, 
          update: fieldUpdate,
          force: true
        })
        cache.markDirtyEntity(toUpdateDestEntity)
      })
    })
  })
}


/**
 * The sourceEntity has been updated to sourceEntityNew by applying an update on a field. Update the nodes connected to sourceEntity and having fields which unionFrom this particular field of sourceEntity. 
 * @param {Object} cache
 * @param {Object} sourceEntity
 * @param {Object} sourceEntityNew
 * @param {String} updatedField
 */
const recalculateUnionInDependentGraph = (cache, sourceEntityOld, sourceEntityNew, updatedField) => {
  const entitySchema = cache.es.config.schema[sourceEntityNew._type]
  const updatedFieldSchema = entitySchema[updatedField]
  const unionInPaths = _([updatedFieldSchema.unionIn]).flatten().compact().value()

  return async.each(unionInPaths, (unionInPath) => {
    return utils.getEntitiesAtPath(cache, sourceEntityNew, _.dropRight(unionInPath, 1))
    .then((toUpdateDestEntities) => {
      if (!toUpdateDestEntities) {
        return
      }
      if (_.isArray(toUpdateDestEntities)) {
        if (!_.size(_.compact(toUpdateDestEntities))) {
          return
        }
      } else { //Make it an array
        toUpdateDestEntities = [toUpdateDestEntities]
      }
      //debug('recalculateUnionInGraph: will update union at path', unionInPath, 'sourceEntityNew', sourceEntityNew._source, 'sourceEntityOld._source', sourceEntityOld, 'to update entities', JSON.stringify(toUpdateDestEntities))
      return async.each(toUpdateDestEntities, (toUpdateDestEntity) => {
        const destFieldToUpdate = _.last(unionInPath)
        return utils.recalculateUnionInSibling(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destFieldToUpdate, cache)
      })
      //.then(() => {
      //  debug(updatedField, sourceEntityNew)
      //})
    })
  })
}

module.exports.updateLeftSubgraph = updateLeftSubgraph
module.exports.recalculateUnionInDependentGraph = recalculateUnionInDependentGraph
module.exports.recalculateJoinsInDependentGraph = recalculateJoinsInDependentGraph
