'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph/inboundUpdater')
const utils = require('../utils')
const traverser = require('./traverser')
const outboundUpdater = require('./outboundUpdater')

/**
 * Updates the leftNode and the nodes linked to it (further down upto traversal depth), based on the dependency paths from those nodes going through the newly established leftNode->rightNode path
 * @param {Object} cache
 * @param {Object} leftNode
 * @param {Array | String} path => [(relationName, nodeId)+] From left node to right node 
 * @param {Object} rightNode
 *
 */
const execute = (cache, leftNode, path, rightNode, updateLeftNode, traversalDepth) => {

  //debug('execute', leftNode, path, rightNode )
  return utils.getEntity(cache, leftNode._id, leftNode._type)
  .then((leftNode) => {

    //Update this node from graph of right
    return updateLeftNode(cache, leftNode, path, rightNode)
    .then(() => {

      //Update its pre-existing graph
      traversalDepth = traversalDepth || cache.es.config.common.graphTraversalDepth
      
      //Update the related nodes to leftNode
      //Recursive callback to be given for traversal
      const updateFurtherLeft = (params) => {

        const relationPathFromVisited = utils.extractRelationWisePath(params.edgePathFromVisited)
        const relationPathFromLeftToRight = utils.extractRelationWisePath(path)

        //debug('updating further left from', leftNode._type, 'visited node', params.visitedNode._type, 'path from visited to node', utils.extractRelationWisePath(params.edgePathFromVisited), ' right Node', rightNode._type, 'visited node source', params.visitedNode._source) 

        return updateLeftNode(cache, params.visitedNode, params.edgePathFromVisited, rightNode)
      }
     
      return traverser.traverseFromNode(cache, leftNode, traversalDepth, path, updateFurtherLeft) 
    })
  })
}

module.exports.execute = execute 
