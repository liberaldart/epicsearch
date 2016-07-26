'use strict'
const debug = require('debug')('crudLink')
const async = require('async-q')
const _ = require('lodash')
const Q = require('q')
const await = require('asyncawait/await');
const suspendable = require('asyncawait/async');

const utils = require('./utils')
const Cache = require('../cache')
const graph = require('./graph')
/**
 * Link entities of a particular type, along a particular relationship,
 * with entities of possibly different types. The linking goes through provided:
 * a. Schema.e1.e1Relation definition exists
 * b. All e2 types are allowed as per schema.e1.e1Relation definition
 * c. If the reverse relationship name is specified in schema.e1.e1Relation,
 *    and is specified in e2 schema,
 *    and it carries e1Relation as the reverse name under which e1Type is valid destination
 *
 * If the operation is not allowed for any combination of e1+e2, then an exception is thrown
 * immediately without applying any changes anywhere.

 * @param {Object} e1
 * @param {String} e1ToE2Relation
 * @param {Object || [Object]} (e2 or e2Entities) single entity or array of objects with _type and _id fields
 * @param {Boolean} isOwn - Whether this link is created by a direct api call to deep.link or from within deep.update? In earlier case isOwn is true. Latter case it must be falsy.
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
  return async.eachSeries(entities, (e2) => {
    return dualLink(params.e1, e2, params.e1ToE2Relation, cache, params.isOwn)
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
        res.e2Entities = await(params.e2Entities.map((e2) => {
          return utils.getEntity(cache, e2._id, e2._type)
        }))
      }
      return res
    })()
  })
  .catch((err) => {
    debug(err, err.stack)
    throw err
  })
}

const dualLink = (e1, e2, e1ToE2Relation, cache, isOwn) => {
  const e1ToE2RelationDef = cache.es.config.schema[e1._type][e1ToE2Relation]
  if (!e1ToE2RelationDef) {
    throw new Error('Relation not found: EntityType: ' + e1._type + '. Relation: ' + e1ToE2Relation)
  }
  const e2ToE1Relation = e1ToE2RelationDef.inName

  return Q.all([
    makeLink(e1, e1ToE2Relation, e2, cache, isOwn),
    makeLink(e2, e2ToE1Relation, e1, cache, isOwn)]
  )
  .spread((res1, res2) => {
    if (res1 != 'already linked' && res2 != 'already linked') {
      debug('just dual linked', e1._type, e1ToE2Relation, e2._type, isOwn)
    }
  })
}

const makeLink = (e1, e1ToE2Relation, e2, cache, isOwn) => {
  if (!e1ToE2Relation) {
    return Q()
  }

  return isAlreadyLinked(e1, e2, e1ToE2Relation, cache)
  .then((isAlreadyLinked) => {
    if (isAlreadyLinked) {//Don't need to link
      //debug('already linked', e1._type, e1ToE2Relation, e2._type)
      return Q('already linked')
    } 

    return setRelationPropertyInE1(cache, e1, e1ToE2Relation, e2, isOwn)
    .then(() => {
      return graph.updateLeftSubgraph(cache, e1, [e1ToE2Relation, e2._id], e2, 5)
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
  return cache.es.deep.update({
    _type: e1._type,
    _id: e1._id,
    update: {
      [linkOp]: {
        [e1ToE2Relation]: {_id: e2._id, own: isOwn}
      }
    }
  }, cache)
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
