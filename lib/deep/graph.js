'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph')

const utils = require('./utils')
/**
 * @param {Object} cache
 * @param {Object} leftNode
 * @param {Array | String} path => [(relationName, nodeId)+] 
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
        if (!leftNode._source) {
          debug(leftNode, relatedNodeInfo.to, rightNode)
        } 
        //Get related Nodes
        let relatedNodes = leftNode._source[relationName]
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

const INFORMATION_DEPENDENCY_TYPES = ['unionFrom', 'copyFrom', 'joinFrom']
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

module.exports.updateLeftSubgraph = updateLeftSubgraph
