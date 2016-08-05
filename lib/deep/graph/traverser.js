'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph')
const updater = require('js-object-updater')

const utils = require('../utils')

/**
 * Traverses the newly connected graph of leftNode through rightNode, based on a particular pathType in fields of leftNode
 * For every node found, applies the callback with following params Object. Returns when callback has been applied to each traversed node
 * {
 *  pathType: {String} //For which we are traversing the newly connected graph
 *  leftNode: {Object}, //in ES format with _source
 *  leftNodeFieldInfo: {Object}, //Info of field for which the pathType was traversed
 *  edgePathToRightNode: [({relationName}, {node._id}),* //Sequence of relationName followed by id]
 *  rightNode: {Object}, //in ES format with _source
 *  relationPathToRightNode: [String]
 * }
 * @param {Object} cache
 * @param {String} pathType either of unionFrom, copyFrom, joinFrom
 * @param {Object} leftNode The node whose field (of which is the fieldInfo) is to be udpated based on another field in rightNode (*if applicable)
 * @param {Array} edgePathToVisitedNode Path consisting of relationship, _id pairs, leading from leftNode to rightNode.
 * @param {Object} visitedNode the source of information from which left Node's field is to be updated (if applicable)
 *
 */
const traverseThroughRightNode = (cache, pathType, leftNode, edgePathToRightNode, rightNode, callback) => {

  const leftNodeSchema = cache.es.config.schema[leftNode._type]
  //For every field of the leftNode - relations and properties
  
  return async.each(_.values(leftNodeSchema), 
    (leftNodeFieldInfo) => traverseForFieldThroughRightNode(cache, pathType, leftNode, leftNodeFieldInfo, edgePathToRightNode, rightNode)
  )
}

const traverseForFieldThroughRightNode = (cache, pathType, leftNodeFieldInfo, leftNode, edgePathToRightNode, rightNode) => {
  //Which paths to fetch depdendent field information from?
  const relationWisePathsForDependency = leftNodeFieldInfo[pathType] //Like unionFrom: ['session','speakers']
  if (!relationWisePathsForDependency) {
    return
  }

  //Get relation wise path from edge path. Ex. sessions.speakers from sessions, 'session_id', speakers, 'speaker_id'
  const relationWisePathToRightNode = extractRelationWisePath(edgePathToRightNode)

  //Extract relation paths emanating from right node, for this pathType 
  const relationWisePathsAheadOfRightNode = 
    relationWisePaths
      .filter((path) => startsWith(path, relationWisePathToRightNode))
      //Remove path to right node from the full path, to get path ahead of right node
      .map((path) => _.drop(path, relationWisePathToRightNode.length))

  //We traverse the new paths exposed by new connection of leftGraph with rightNode and its graph. So we start to walk from rightNode.
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
        return callback({
          cache: cache,
          pathType: pathType,
          leftNode: leftNode,
          leftNodeFieldInfo: leftNodeFieldInfo, 
          edgePathToRightNode: edgePathToRightNode,
          relationPathToRightNode: relationWisePathToRightNode,
          rightNode: rightNode,
          rightNodeField: _.last(relationPathAheadOfRightNode)
        })
      })
    })
  })
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
 * Traverse all the relationships through this node, and their relationships
 * upto maximum depth of traversalDepth
 */
const traverseFromNode = (cache, node, traversalDepth, edgePathSoFar, callback) => {

  if (!traversalDepth || !(node._source || node.fields)) {
    return Q()
  }

  //For every relation definition of left node
  const relations = utils.relationships(cache, node._type)
  return async.each(_.keys(relations), (relationName) => {
    const relatedNodeInfo = cache.es.config.schema[node._type][relationName]

    //For every related node connected to left node under that relationship definition
    //Get related Nodes
    let relatedNodes = (node._source || node.fields)[relationName]
    if (!relatedNodes) {
      return Q()
    }
    
    if (!_.isArray(relatedNodes)) {
      relatedNodes = [relatedNodes]
    }

    relatedNodes = relatedNodes.map((node) => {return {_id: node._id, _type: relatedNodeInfo.to}})

    //For each such node execute the callback
    return async.each(relatedNodes, (relatedNode) => {

      const edgePathToVisited = [relatedNodeInfo.inName, node._id].concat(edgePathSoFar) //From the very original node from which the edgePathSoFar begins
      return  callback({
        cache: cache,
        node: node,
        visitedNode: relatedNode,
        edgePathToVisited: edgePathToVisited
      })
      .then(() => {
        return traverseFromNode(cache, relatedNode, traversalDepth - 1, edgePathToVisited, callback)
      })
    })
  })
}


module.exports.traverseThroughRightNode = traverseThroughRightNode
module.exports.traverseFromNode = traverseFromNode
