'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/graph/traverser')
const updater = require('js-object-updater')

const utils = require('../utils')

/**
 * Traverses the newly connected graph of leftNode through rightNode, based on a particular pathType in fields of leftNode
 * For every node found, applies the callback with following params Object. Returns when callback has been applied to each traversed node
 * {
 *  pathType: {String} //For which we are traversing the newly connected graph. Ex. unionIn, joinIn
 *  leftNode: {Object}, //in ES format with _source
 *  leftNodeFieldInfo: {Object}, //Info of field for which the pathType was traversed
 *  //edgePathToRightNode: [({relationName}, {node._id}),* //Sequence of relationName followed by id]
 *  rightGraphNode: {Object}, //in ES format with _source
 *  relationPathToRightGraphNode: [String]
 *  rightGraphNodeField: String
 * }
 * @param {Object} cache
 * @param {String} pathType either of unionFrom, copyFrom, joinFrom
 * @param {Object} leftNode The node whose field (of which is the fieldInfo) is to be udpated based on another field in rightNode (*if applicable)
 * @param {Array} edgePathToRightNode Path consisting of relationship, _id pairs, leading from leftNode to rightNode.
 * @param {Object} visitedNode the source of information from which left Node's field is to be updated (if applicable)
 *
 */
const traverse = (cache, pathType, leftNode, edgePathToRightNode, rightNode, callback) => {

  const leftNodeSchema = cache.es.config.schema[leftNode._type]

  return utils.loadEntities(cache, leftNode, rightNode)
  .then(() => {
    debug('traverseThroughRightNode: Left node', leftNode._type, 'path to right', edgePathToRightNode, 'right node', rightNode._type)

    return async.each(_.keys(leftNodeSchema),
      (leftNodeFieldName) => {
        const leftNodeFieldInfo = leftNodeSchema[leftNodeFieldName]
        return traverseForField(cache, pathType, leftNodeFieldInfo, leftNodeFieldName, leftNode, edgePathToRightNode, rightNode, callback)
      }
    )
  })
  //For every field of the leftNode - relations and properties
}

const traverseForField = (cache, pathType, leftNodeFieldInfo, leftNodeFieldName, leftNode, edgePathToRightNode, rightNode, callback) => {

  //Get relation wise path from edge path. Ex. sessions.speakers from sessions, 'session_id', speakers, 'speaker_id'
  const pathToRightNode = utils.extractRelationWisePath(edgePathToRightNode)

  //Which paths to fetch depdendent field information from?
  let pathsStartingFromLeftNode =
    _.get(leftNodeFieldInfo[pathType], ['startingFromMe', 'index', leftNode._type, leftNodeFieldName, 'paths'])
  pathsStartingFromLeftNode = pathsStartingFromLeftNode && pathsStartingFromLeftNode.filter((path) => utils.indexOf(path, pathToRightNode) > -1)

  if (!_.isEmpty(pathsStartingFromLeftNode)) {
    return async.each(pathsStartingFromLeftNode, (pathStartingFromLeftNode) => {
      return traverseForFieldForPath(cache, pathType, leftNode, leftNodeFieldInfo, leftNodeFieldInfo.name, pathToRightNode, rightNode, pathStartingFromLeftNode, callback)
    })
  }

  const goingThroughContextInfo = _.get(leftNodeFieldInfo[pathType], ['throughMe', 'index'])
  //leftNode._type, leftNodeFieldName, 'paths'])
  goingThroughContextInfo && _.each(goingThroughContextInfo, (fieldWiseInfo, et) => {
    _.each(fieldWiseInfo, (fieldInfo, fieldName) => {
      const pathsToTraverse = fieldInfo.paths && fieldInfo.paths.filter((path) => utils.indexOf(path, pathToRightNode) > -1)
      if (!pathsToTraverse) {
        return
      }
      return async.each(pathsToTraverse, (pathThroughLeftNode) => {
        return traverseForFieldForPath(cache, pathType, leftNode, leftNodeFieldInfo, leftNodeFieldInfo.name, pathToRightNode, rightNode, pathThroughLeftNode, callback)
      })
    })
  })


  //debug(pathsStartingFromLeftNode, pathsGoingThroughLeftNode, leftNode._type, leftNodeFieldInfo.name, JSON.stringify(leftNodeFieldInfo))
  //Extract relation paths emanating from right node, for this pathType
  const pathsThroughLeftAndRight =
    _.union(pathsStartingFromLeftNode || [], pathsGoingThroughLeftNode || [])

  if (_.isEmpty(pathsThroughLeftAndRight)) {
    return
  }

  //We traverse the new paths exposed by new connection of leftGraph with rightNode and its graph. So we start to walk from rightNode.
  //For each of matched paths
}

const traverseForFieldForPath = (cache, pathType, leftNode, leftNodeFieldInfo, leftMostNodeField, pathToRightNode, rightNode, pathThroughLeftAndRight, callback) => {
  return getleftMostNodes(cache, pathType, leftNode, leftNodeFieldInfo, pathToRightNode, pathThroughLeftAndRight)
  .then((leftMostNodes) => {
    if (!leftMostNodes) {
      return
    }
    //debug(pathType, leftNode._type, leftNodeFieldInfo.name, pathToRightNode, rightNode._type, pathThroughLeftAndRight)
    return async.each(leftMostNodes, (leftMostNode) => {
      //Get leaf entities at of that path when exploring from right node.
      const relationPathTorightMostNodes = _.dropRight(pathThroughLeftAndRight, 1)
      //debug(leftMostNode, relationPathTorightMostNodes)
      return utils.getEntitiesAtRelationPath(cache, leftMostNode, relationPathTorightMostNodes)
      .then((rightMostNodes) => {
        //debug('rightMostnodes',  leftMostNode._type, relationPathTorightMostNodes, rightMostNodes)

        //The right node's field to udpate from
        //Can be a simple field or a relationship field. Doesn't matter

        return async.each(rightMostNodes.entities, (rightMostNode) => {
          const params = {
            cache: cache,
            pathType: pathType,
            leftNode: leftMostNode,
            leftNodeFieldInfo: cache.es.config.schema[leftMostNode._type][leftMostNodeField],
            relationPathToRightGraphNode: _.dropRight(pathThroughLeftAndRight, 1),
            edgePathToRightNode: rightMostNodes.idToEdgePathMap[rightMostNode._id],
            rightGraphNode: rightMostNode,
            rightGraphNodeField: _.last(pathThroughLeftAndRight)
          }
          return callback(params)
        })
      })
    })
  })
}

const getleftMostNodes = (cache, pathType, leftNode, leftNodeFieldInfo, pathToRightNode, pathThroughLeftAndRight) => {
  const indexOfleftNode = utils.indexOf(pathThroughLeftAndRight, pathToRightNode)
  const pathFromleftMostToLeftNode = _.slice(pathThroughLeftAndRight, 0, indexOfleftNode)
  return async.each(_.keys(leftNodeFieldInfo[pathType]['throughMe']['index']), (et) => {
    const pathFromLeftToleftMostNode = utils.reverseRelationPath(cache.es.config, et, pathFromleftMostToLeftNode).reversePath
    return utils.getEntitiesAtRelationPath(cache, leftNode, pathFromLeftToleftMostNode)
    .then((response) => response.entities)
  })
  .then((result) => _.flatten(result))
}

if (require.main === module) {
  const EpicSearch = require('../../../index')
  debug(process.argv)
  const es = new EpicSearch(process.argv[2])
  const Cache = require('../../cache')
  const cache = new Cache(es)
  traverse(cache, 'unionFrom', {_id: 1, _type: 'session'}, ['speakers', 1], {_type: 'speaker', _id: 1}, (params) => debug(_.omit(params, 'cache')))
  .catch(debug)
}

/**
 * Note: Ununsed for now. So not exported
 * Traverse all the relationships through this node, and their relationships
 * upto maximum depth of traversalDepth
 * Invokes the callback with params
 *
 * cache: {},
 * node: {},
 * visitedNode:{},
 * edgePathFromVisited: [(_type,_id)+]
 */
const traverseFromNode = (cache, node, traversalDepth, edgePathFromNode, callback) => {
  debug('traverseFromNode: Node', node._type, 'edgePathFromNode', edgePathFromNode)
  //Load the node
  return utils.getEntity(cache, node._id, node._type)
  .then((loadedNode) => {
    node = loadedNode

    if (!traversalDepth || !(node._source || node.fields)) {
      return Q()
    }

    //For every relation definition of left node
    const relations = utils.relationships(cache, node._type)
    return async.each(_.keys(relations), (relationName) => {

      const relatedNodeInfo = cache.es.config.schema[node._type][relationName]
      //debug(relationName, edgePathFromNode, node)
      if (relationName === edgePathFromNode[0]) { //Avoid retracing to original node to which edgePathFromNode takes from node
        return Q()
      }
      //For every related node connected to left node under that relationship definition
      //Get related Nodes
      let relatedNodes = (node._source || node.fields)[relationName]
      if (!relatedNodes) {
        return Q()
      }
      if (!_.isArray(relatedNodes)) {
        relatedNodes = [relatedNodes]
      }

      relatedNodes = relatedNodes.map(
        (relatedNode) => {

          if (!relatedNode._id) {
            throw new Error('no id in relatedNode' + JSON.stringify(relatedNode) + 'original node' + JSON.stringify(node))
          }

          return {_id: relatedNode._id, _type: relatedNodeInfo.to}
        })
      //debug('traversing from node', node._type, 'with path to original node', edgePathFromNode, 'at depth', traversalDepth, 'for relation', relationName, 'related nodes', relatedNodes)

      //For each such node execute the callback
      return async.each(relatedNodes, (relatedNode) => {

        const edgePathFromRelated = [relatedNodeInfo.inName, node._id].concat(edgePathFromNode)
        //debug('About to visit ' + relatedNode._type + ' whose path to right node is ' + edgePathFromRelated)
        //debug(edgePathToVisited, relatedNodeInfo)
        return callback({
          cache: cache,
          node: node,
          visitedNode: relatedNode,
          edgePathFromVisited: edgePathFromRelated
        })
        .then(() => {
          if (traversalDepth === 1) {
            return Q()
          }
          //debug('about to traverse from relatedNode', relatedNode._type, edgePathFromRelated, 'traversal depth remaining ', traversalDepth - 1)
          return traverseFromNode(cache, relatedNode, traversalDepth - 1, edgePathFromRelated, callback)
        })
      })
    })
  })
}

module.exports = traverse
