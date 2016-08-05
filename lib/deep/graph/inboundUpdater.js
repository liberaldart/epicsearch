'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph')
const updater = require('js-object-updater')

const utils = require('../utils')
const traverser = require('./traverser')

/**
 * Updates the leftNode and the nodes linked to it (further down upto traversal depth), based on the dependency paths from those nodes going through the newly established leftNode->rightNode path
 * @param {Object} cache
 * @param {Object} leftNode
 * @param {Array | String} path => [(relationName, nodeId)+] From left node to right node 
 * @param {Object} rightNode
 *
 */
const execute = (cache, leftNode, path, rightNode, traversalDepth) => {


  return utils.getEntity(cache, leftNode._id, leftNode._type)
  .then((leftNode) => {
    return updateLeftNode(cache, leftNode, path, rightNode)
    .then(() => {
    
      traversalDepth = traversalDepth || cache.es.config.common.graphTraversalDepth
      
      //Update the related nodes to leftNode
      const updateFurtherLeft = (params) => {
        return execute(params.cache, params.visitedNode, params.edgePathSoFar, rightNode, traversalDepth - 1) 
      }
     
      return traverser.traverseFromNode(cache, leftNode, traversalDepth -1, [], updateFurtherLeft) 
    })
  })
}

const INFORMATION_DEPENDENCY_TYPES = ['unionFrom', 'joinFrom']
const updateLeftNode = (cache, leftNode, edgePathToRightNode, rightNode) => {
  //Update this node based on each type of dependency
  return async.each(INFORMATION_DEPENDENCY_TYPES, (dependencyType) => {
  return traverser.traverseThroughRightNode(cache, dependencyType, leftNode, edgePathToRightNode, rightNode, updateFromRightNode)
  })
}

const updateFromRightNode = (params) => {
  switch(params.pathType) {
    case 'unionFrom': {
      return utils.recalculateUnionInSibling(null, params.rightNode, params.rightNodeField, params.leftNode, params.leftNodeFieldInfo.name, cache)

    }
  }
  
}

module.exports.execute = execute 
