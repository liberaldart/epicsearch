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

  return updateLeftNode(cache, leftNode, path, rightNode)
  .then(() => {

    //For every relation definition of left node
    const leftNodeRelations = utils.relationships(cache, leftNode._type)
    return async.eachSeries(_.keys(leftNodeRelations), (relationName) => {

      //For every related node under that relationship definition
      const relatedNodes = leftNode[relationName]
      if (!relatedNodes) {
        return Q()
      }
      return async.eachSeries(relatedNodes, (relatedNode) => {
        return updateLeftSubgraph(cache, relatedNode, [relationName, node._id].concat(path), rightNode) 
      })
    })

  })
}

const INFORMATION_DEPENDENCY_TYPES = ['unionFrom', 'copyFrom', 'joinFrom']
const updateLeftNode = (cache, leftNode, edgePathToRightNode, rightNode) => {
  const leftNodeSchema = cache.es.config.schema[leftNode._type]
  //For every field of the leftNode - relations and properties
  return async.eachSeries(_.values(leftNodeSchema), (leftNodeFieldInfo) => {
    //Update this field based on each type of dependency
    return async.eachSeries(INFORMATION_DEPENDENCY_TYPES, (dependencyType) => {
      return updateFieldFromRightGraph(cache, dependencyType, leftNode, leftNodeFieldInfo, edgePathToRightNode, rightNode)
    })
  })
}


//Update this field based on each type of dependency
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
  const relationWisePathsAheadOfRightNode = 
    relationWisePathsForDependency.filter((path) =>  startsWith(path, relationWisePathToRightNode))
    .map((relationWisePathThroughRightNode) => 
    
      //We traverse the new paths exposed by new connection of leftGraph with rightNode and its graph. So we start to walk from rightNode.
      _(relationWisePathThroughRightNode).drop(relationWisePathToRightNode.length).dropRight(1).value() //Cut out the path ahead of rightNode, and from that remove the last edge of the path. The last edge is the field of leaf nodes from which the update is to be done in leftNode's field 

  )

  return udpateFieldFromPathsThroughRightNode(cache, dependencyType, leftNode, leftNodeFieldInfo, edgePathToRightNode, rightNode, relationWisePathsAheadOfRightNode)
}

const udpateFieldFromPathsThroughRightNode = (cache, dependencyType, leftNode, leftNodeFieldInfo, edgePathToRightNode, rightNode, relationPathsAheadOfRightNode) => {

  //For each of matched paths
  return async.eachSeries(relationPathsAheadOfRightNode, (relationPathAheadOfRightNode) => {

    //Get leaf entities at of that path when exploring from right node.
    return utils.getEntitiesAtPath(cache, rightNode, relationPathAheadOfRightNode) 
    .then((nodesFromRightGraph) => {

      //The right node's field to udpate from
      //Can be a simple field or a relationship field. Doesn't matter
      const rightNodeField = _.last(relationPathAheadOfRightNode)

      return async.eachSeries(nodesFromRightGraph, (rightNode) => {
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
