'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph/inboundUpdater')
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
    //Update this node from graph of right
    return updateLeftNode(cache, leftNode, path, rightNode)
    .then(() => {
      //Update its pre-existing graph
      traversalDepth = traversalDepth || cache.es.config.common.graphTraversalDepth
      
      //Update the related nodes to leftNode
      //Recursive callback to be given for traversal
      const updateFurtherLeft = (params) => {
        //debug('updating further left', _.omit(params, 'cache'))
        //return execute(params.cache, params.visitedNode, params.edgePathFromVisited, rightNode, traversalDepth - 1) 
        return updateLeftNode(cache, params.visitedNode, params.edgePathFromVisited, rightNode)
      }
     
      return traverser.traverseFromNode(cache, leftNode, traversalDepth, path, updateFurtherLeft) 
    })
  })
}

const updateLeftNode = (cache, leftNode, edgePathToRightNode, rightNode) => {
  return updateLeftNodeRelationsWithRightNode(cache, leftNode, edgePathToRightNode, rightNode)
    .then(() => updateLeftNodeRelationsFromRightGraph(cache, leftNode, edgePathToRightNode, rightNode))
}

/**
 * if any of leftNode's relations (through a connected node just before right node), need to have rightNode in them (unionIn etc.), then copy rightNode there.
 */
const updateLeftNodeRelationsWithRightNode = (cache, leftNode, edgePathToRightNode, rightNode) => {
  if (edgePathToRightNode.length === 2) { //rightNode is immediate neighbor
    return Q()
  }
  
  const leftNodeSchema = cache.es.config.schema[leftNode._type]
  const relationPathToRightNode = utils.extractRelationWisePath(edgePathToRightNode)

  const leftNodeRelations = utils.relationships(cache, leftNode._type)
  return async.each(_.keys(leftNodeSchema), (field) => {
    //Deal with only relationships here
    if (!leftNodeSchema[field].isRelationship) {
      return
    }
    const relation = field
    const relationInfo = leftNodeSchema[relation]
      
    return async.each(INFORMATION_DEPENDENCY_TYPES, (dependencyType) => {
      const dependencyPaths = relationInfo[dependencyType]
      if (!_.find(dependencyPaths, relationPathToRightNode)) {
        return
      }

      const edgePathToSecondLastNode = _.dropRight(edgePathToRightNode, 2)

      return utils.getEntityAtEdgePath(cache, leftNode, edgePathToSecondLastNode)
      .then((entityJustBeforeRightNode) => {
        const updateParams = {
          cache: cache,
          pathType: dependencyType,
          leftNode: leftNode,
          leftNodeFieldInfo: relationInfo, 
          rightGraphNode: entityJustBeforeRightNode,
          relationPathToRightGraphNode: _.dropRight(relationPathToRightNode, 1),
          rightGraphNodeField: _.last(relationPathToRightNode)
        
        }
        return applyUpdate(updateParams)
      })
    })
  })
}

const INFORMATION_DEPENDENCY_TYPES = ['unionFrom', 'joinFrom']
const updateLeftNodeRelationsFromRightGraph = (cache, leftNode, edgePathToRightNode, rightNode) => {
  if (leftNode._type === rightNode._type) { //Avoid cycles
    return Q()
  }
  //debug('Visiting ' + leftNode._type + ' whose path to right Node type ' + rightNode._type + ' is '  + edgePathToRightNode)
  //Update this node based on each type of dependency
  return async.each(INFORMATION_DEPENDENCY_TYPES, (dependencyType) => {
    return traverser.traverseThroughRightNode(cache, dependencyType, leftNode, edgePathToRightNode, rightNode, applyUpdate)
  })
}

const applyUpdate = (params) => {
  switch(params.pathType) {
    case 'unionFrom': {
      return utils.getEntity(params.cache, params.rightGraphNode._id, params.rightGraphNode._type)
      .then((rightNodeLoaded) => {
        params.rightGraphNode = rightNodeLoaded
        return utils.getEntity(params.cache, params.leftNode._id, params.leftNode._type)
        
      })
      .then((leftNodeLoaded) => {
        params.leftNode = leftNodeLoaded

        return utils.recalculateUnionInSibling(null, params.rightGraphNode, params.rightGraphNodeField, params.leftNode, params.leftNodeFieldInfo.name, params.cache)
      })
    }
    default: {
      return Q()
    }
  }
  
}

module.exports.execute = execute 
