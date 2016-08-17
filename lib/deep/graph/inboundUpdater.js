'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph/inboundUpdater')
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

  //debug('execute', leftNode, path, rightNode)
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

const updateLeftNode = (cache, leftNode, edgePathToRightNode, rightNode) => {
  //debug('exploring possible update of node', leftNode._type, 'from (including its graph)', rightNode._type, 'with path', edgePathToRightNode)
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
  //debug(leftNode, edgePathToRightNode, rightNode)
  
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
      if (!utils.containsPath(dependencyPaths, relationPathToRightNode)) {
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
          rightGraphNodeOld: getOldValue(cache, entityJustBeforeRightNode, _.last(relationPathToRightNode), rightNode, true),
          relationPathToRightGraphNode: _.dropRight(relationPathToRightNode, 1),
          rightGraphNodeField: _.last(relationPathToRightNode)
        }
        //debug('updating field', updateParams.leftNodeFieldInfo.name, 'of', leftNode._type, 'from field', updateParams.rightGraphNodeField, 'of right node', updateParams.rightGraphNode._type, '. Path to right node', updateParams.relationPathToRightGraphNode)
        return applyUpdate(updateParams)
      })
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

const INFORMATION_DEPENDENCY_TYPES = ['unionFrom', 'joinFrom']
const updateLeftNodeRelationsFromRightGraph = (cache, leftNode, edgePathToRightNode, rightNode) => {
  if (leftNode._type === rightNode._type) { //Avoid cycles
    return Q()
  }
  //debug('updating node', leftNode._type, 'from graph of', rightNode._type, 'with path to right node', utils.extractRelationWisePath(edgePathToRightNode))
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

        debug('recalculating union of', params.leftNodeFieldInfo.name, 'in', params.leftNode._type, 'from', params.rightGraphNode._type)

        return utils.recalculateUnionInSibling(params.rightGraphNodeOld, params.rightGraphNode, params.rightGraphNodeField, params.leftNode, params.leftNodeFieldInfo.name, params.cache)
      })
    }
    default: {
      return Q()
    }
  }
}

module.exports.execute = execute 
