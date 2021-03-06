'use strict'
const debug = require('debug')('epicsearch:deep/link')
const async = require('async-q')
const _ = require('lodash')
const Q = require('q')
const await = require('asyncawait/await');
const suspendable = require('asyncawait/async');

const utils = require('./utils')
const Cache = require('../cache')
const inboundGraphUpdater = require('./graph/inboundUpdater')
const unionCalculator = require('./graph/unionCalculator')
const joinCalculator = require('./graph/joinCalculator')
const traverser = require('./graph/traverser')
/**
 * Link entities of a particular type, along a particular relationship,
 * with entities of possibly different types. The linking goes through provided:
 * a. Schema.e1.e1Relation definition exists
 * b. All e2 types are allowed as per schema.e1.e1Relation definition
 * c. If the reverse relationship name is specified in schema.e1.e1Relation,
 *    and is specified in e2 schema,
 *    ande it carries e1Relation as the reverse name under which e1Type is valid destination
 *
 * If the operation is not allowed for any combination of e1+e2, then an exception is thrown
 * immediately without applying any changes anywhere.

 * @param {Object} e1
 * @param {String} e1ToE2Relation
 * @param {Object || [Object]} (e2 or e2Entities) single entity or array of objects with _type and _id fields
 * @param {Boolean} isOwn - Whether this link is created by a direct api call to deep.link or from within deep.update? In earlier case isOwn is true. Latter case it must be falsy.
 * @param {Boolean} relationPropertyIsSet - If true, it is assumed that e2Entities or e2, is already set in e1ToE2Relation. So deep.update is   not called to set the relationship property }
 *
 */
function Index(es) {
  this.es = es
}

Index.prototype.execute = function(params, cache) {
  const es = this.es
  const config = this.es.config
  let indexEntitiesAtEnd = false
  if (!cache) {
    cache = new Cache(es)
    indexEntitiesAtEnd = true
  }
  const entities = _.flatten([params.e2 || params.e2Entities])
  return async.each(entities, (e2) => {
    return isAlreadyDualLinked(params.e1, e2, params.e1ToE2Relation, cache)
    .then((isAlreadyDualLinked) => {

      if (isAlreadyDualLinked) {

        //debug('already dual linked e1', params.e1._type, 'on relation', params.e1ToE2Relation, 'e2', params.e2._type)    

      } else {

        //debug('starting dual linked. E1', params.e1._type, 'e2', e2._type, 'isOwn', params.isOwn)
        return dualLink(params.e1, e2, params.e1ToE2Relation, cache, params.isOwn, params.relationPropertyIsSet)
      }
    })
  })
  .then(() => {
    if (indexEntitiesAtEnd) {
      return cache.flush()
    } else {
      return Q()
    }
  })
  .then(() => {
    return suspendable(() => {
      const res = {
        e2ToE1Relation: params.e1ToE2Relation,
        e1: await(utils.getEntity(cache, params.e1._id, params.e1._type))
      }
      if (params.e2) {
        res.e2 = await(utils.getEntity(cache, params.e2._id, params.e2._type))
      } else {
        res.e2Entities = await(async.map(params.e2Entities, (e2) => 
          utils.getEntity(cache, e2._id, e2._type)
        ))
      }
      return res
    })()
  })
}

const dualLink = (e1, e2, e1ToE2Relation, cache, isOwn, relationPropertyIsSet) => {
  const e1ToE2RelationDef = cache.es.config.schema[e1._type][e1ToE2Relation]
  if (!e1ToE2RelationDef) {
    throw new Error('Relation not found: EntityType: ' + e1._type + '. Relation: ' + e1ToE2Relation)
  }
  const e2ToE1Relation = e1ToE2RelationDef.inName
  return makeLink(e1, e1ToE2Relation, e2, cache, isOwn, relationPropertyIsSet)
  .then(() => makeLink(e2, e2ToE1Relation, e1, cache, isOwn, relationPropertyIsSet))
  /**.spread((res1, res2) => {
    if (res1 != 'already linked' && res2 != 'already linked') {
      debug('already linked',  e1._type, e1ToE2Relation, e2._type, isOwn) 
    }
  })**/
}

const makeLink = (e1, e1ToE2Relation, e2, cache, isOwn, relationPropertyIsSet) => {
  if (!e1ToE2Relation) {
    return Q()
  }

  return isAlreadyLinked(e1, e2, e1ToE2Relation, cache)
  .then((isAlreadyLinked) => {
    //debug('Linking and applying inbound update from e2. E1', e1._type, 'e1ToE2Relation', e1ToE2Relation, 'e2', e2._type, relationPropertyIsSet)
    return (relationPropertyIsSet && Q() || setRelationPropertyInE1(cache, e1, e1ToE2Relation, e2, isOwn))
    .then(() => {
      //debug(e1._type, e2._type, e2._id)
      return e1UnionsAndJoins(cache, e1, e1ToE2Relation, e2)
    })
  })
}

const e1UnionsAndJoins = (cache, e1, e1ToE2Relation, e2) => {

  const edgePathToE2 = [e1ToE2Relation, e2._id]
  //debug('e1UnionsAndJoins', e1Loaded, edgePathToE2, e2Loaded)
  //Recalculate unions in the graph having e1 connected with e2 through e1ToE2Relation
  return traverser(cache, 'unionFrom', e1, edgePathToE2, e2, (params) => {

    const rightGraphNodeData = (params.rightGraphNode.fields || params.rightGraphNode._source)
    const rightGraphNodeOld = getOldValue(cache, params.rightGraphNode, params.rightGraphNodeField, rightGraphNodeData[params.rightGraphNodeField], true)

    return unionCalculator.recalculateUnionInSibling(rightGraphNodeOld, params.rightGraphNode, params.rightGraphNodeField, params.leftNode, params.leftNodeFieldInfo.name, params.cache)
  })
  .then(() => {

    return traverser(cache, 'joinFrom', e1, edgePathToE2, e2, (params) => {
      //debug(params.edgePathToRightNode, 'dddddddddddddddd')

      return joinCalculator.resolveForEntity(cache, null, 'index', params.leftNode, {[params.edgePathToRightNode[0]]: {_id: params.edgePathToRightNode[1]}}, params.edgePathToRightNode)
    })
  })
  .then(() => {
    debug('done e1Unions and joins', e1._type, e2._type)
  })
}

/**
 * @param entry {Object || String || Array} Can be anything as per the relation schema
 * @param updateAction {Boolean} 
 */
const getOldValue = (cache, node, field, entry, wasJustAdded) => {

  const fieldSchema = cache.es.config.schema[node._type][field]
  const latestEntries = (node._source || node.fields)[field]
  let oldSource = _.cloneDeep(node._source || node.fields)

  if (wasJustAdded) {
    if (_.isArray(fieldSchema.type)) {
      const changedEntries = _.isArray(entry) && entry || [entry]
      oldSource[field]  = _.filter(latestEntries, (e) => {
        if (fieldSchema.isRelationship) {
          return !_.find(changedEntries, {_id: e._id})
        } else {
          return !_.includes(changedEntries, e)
        }
      })
    } else {
      oldSource[field]  = undefined
    }
  } else { //was just removed
    oldSource[field] = fieldSchema.cardinality === 'many' && latestEntries.push(entry) || entry
  }
  
  const oldNode = _.omit(node, '_source')
  oldNode._source = oldSource
  return oldNode
}

const isAlreadyDualLinked = (e1, e2, e1ToE2Relation, cache) => {
  return isAlreadyLinked(e1, e2, e1ToE2Relation, cache)
  .then((e1LinkedToE2) => {

    if (!e1LinkedToE2) {
      return false
    }

    const e2ToE1Relation = cache.es.config.schema[e1._type][e1ToE2Relation].inName
    return isAlreadyLinked(e1, e2, e2ToE1Relation, cache)
    .then((e2LinkedToE1) => {
      return e2LinkedToE1
    })
  })
}

const isAlreadyLinked = (e1, e2, e1ToE2Relation, cache) => {
  return utils.getEntity(cache, e1._id, e1._type)
  .then((e1Full) => {
    return _.find(_.flatten([e1Full._source[e1ToE2Relation]]), _.pick(e2, '_id'))
  })
}

const setRelationPropertyInE1 = (cache, e1, e1ToE2Relation, e2, isOwn) => {

  const e1Schema = cache.es.config.schema[e1._type]
  const relationSchema = e1Schema[e1ToE2Relation]
  if (!relationSchema) {
    throw new Error('makeLink: ' + e1ToE2Relation + ' not found in ' + e1._type + ' schema')
  }
  const linkOp = _.isArray(relationSchema.type) && 'addToSet' || 'set'
  //debug('about to set link in e1', linkOp, e1._type, e1._id, e1ToE2Relation, 'isOwn', isOwn)
  return cache.es.deep.update({
    _type: e1._type,
    _id: e1._id,
    update: {
      [linkOp]: {
        [e1ToE2Relation]: {_id: e2._id, own: isOwn}
      }
    },
    isOwn: isOwn,
    dontHandleLinking: true 
  }, cache)
  /**.then((res) => {
    debug('after setting link in e1', linkOp, e1._type, e1._id, e1ToE2Relation, 'with e2 id', e2._id, 'e1 relation prperty is', cache.get(e1._id + e1._type))
    return res
  })**/
}

module.exports = Index

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')
  const Cache = require('../cache')
  const cache = new Cache(es)
  debug(JSON.stringify(cache.es.config.schema))
  es.deep.link({
    e2: {
      _type: 'session',
      _id: '1'
    },
    e1: {
      _type: 'event',
      _id: '1'
    },
    e1ToE2Relation: 'sessions'
  }, cache)
  .then(function(res) {
    debug('cache data', JSON.stringify(cache.data))
    debug('res', JSON.stringify(res))
    return cache.flush()
  })
  /**.then(() => {
    return es.dsl.execute(['get session 1'])
  })
  .then((res) => {debug(JSON.stringify(res))})
  .then(() => {
    return es.dsl.execute(['get event 1'])
  })
  .then((res) => {debug(JSON.stringify(res))})
  .then(() => {
    return es.dsl.execute(['get speaker 1'])
  })
  .then((res) => {debug(JSON.stringify(res))})**/
  .catch(debug)
}
