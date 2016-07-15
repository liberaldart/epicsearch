'use strict'
const debug = require('debug')('crudLink')
const async = require('async-q')
const _ = require('lodash')
const Q = require('q')
const await = require('asyncawait/await');
const suspendable = require('asyncawait/async');

const utils = require('./utils')
const Cache = require('../cache')
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

 * @params {Entity} e1
 * @params {String} e1ToE2Relation
 * @params {Entity or Array of entities} (e2 or e2Entities) single entity or array of objects with _type and _id fields
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
    if (params.e1ToE2Relation) {
      return dualLink(params.e1, e2, params.e1ToE2Relation, cache, config)
    } else {
      return dualLink(e2, params.e1, params.e2ToE1Relation, cache, config)
    }
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

const dualLink = (e1, e2, e1ToE2Relation, cache, config) => {
  const e1ToE2RelationDef = config.schema[e1._type][e1ToE2Relation]
  if (!e1ToE2RelationDef) {
    throw new Error('Relation not found: EntityType: ' + e1._type + '. Relation: ' + e1ToE2Relation)
  }
  const e2ToE1Relation = e1ToE2RelationDef.inName

  return Q.all([
    makeLink(e1, e1ToE2Relation, e2, cache, config),
    makeLink(e2, e2ToE1Relation, e1, cache, config)]
  )
}

const makeLink = (e1, e1ToE2Relation, e2, cache, config) => {
  if (!e1ToE2Relation) {
    return Q()
  }
  return isAlreadyLinked(e1, e2, e1ToE2Relation, cache)
  .then((isAlreadyLinked) => {
    if (isAlreadyLinked) {//Don't need to link
      debug('makeLink. Is already linked. Stopping here', e1._type, e1._id, e1ToE2Relation, e2._id)
      return Q()
    } else {
      debug('makeLink: Adding new Link', e1._type, e1._id, e1ToE2Relation, e2._id)
    }
    const e1Schema = config.schema[e1._type]
    const relationSchema = e1Schema[e1ToE2Relation]
    if (!relationSchema) {
      throw new Error('makeLink: ' + e1ToE2Relation + ' not found in ' + e1._type + ' schema')
    }
    const linkOp = _.isArray(relationSchema.type) && 'addToSet' || 'set'
    //debug(e1._type, e1ToE2Relation, linkOp, JSON.stringify(_.pick(e2.fields, [e1ToE2Relation])))
    return cache.es.deep.update({
      _type: e1._type,
      _id: e1._id,
      update: {
        [linkOp]: {
          [e1ToE2Relation]: _.pick(e2, '_id')
        }
      }
    }, cache)
    .then(() => {
      return updateSibling(e1, e2, e1ToE2Relation, cache, config)
    })
  })
}

const isAlreadyLinked = (e1, e2, e1ToE2Relation, cache) => {
  return utils.getEntity(cache, e1._id, e1._type)
  .then((e1Full) => {
    return _.find(_.flatten([e1Full._source[e1ToE2Relation]]), _.pick(e2, '_id'))
  })
}

//Based on e1's graph and union constrains between that graph nodes and e2
const updateE2OnNewLinkWithE1 = (e2, e1, e1ToE2Relation, cache) => {
  const config = cache.es.config
  const e1ToE2RelationSchema = config.schema[e1._type][e1ToE2Relation]
  const e2ToE1Relation = e1ToE2RelationSchema.inName
  const e2Schema = config.schema[e2._type]
  return async.each(_.keys(e2Schema), (e2Field) => {
    let e2FieldUnionFromPaths = e2Schema[e2Field].unionFrom
    if (!e2FieldUnionFromPaths) {
      return Q()
    }
    e2FieldUnionFromPaths = _.filter(e2FieldUnionFromPaths, (unionFromPath) => _.includes(unionFromPath, e2ToE1Relation))
    if (e2FieldUnionFromPaths.length) {
      return async.each(e2FieldUnionFromPaths, (e2FieldUnionFromPath) => {
        e2FieldUnionFromPath = e2FieldUnionFromPath.split('.')
        return suspendable(() => {
          let entitiesToUnionFrom = await(
            utils.getEntitiesAtPath(cache, e1, e2FieldUnionFromPath.splice(1, e2FieldUnionFromPath.length - 2))
          )
          entitiesToUnionFrom = _.flatten([entitiesToUnionFrom])
          return async.each(entitiesToUnionFrom, (entityToUnionFrom) => {
            return utils.recalculateUnionInSibling(null, entityToUnionFrom, _.last(e2FieldUnionFromPath), e2, e2Field, cache)
          })
        })()
      })
    }
  })
}

const updateE2GraphOnNewLinkWithE1 = (e2, e1, e1ToE2Relation, cache) => {
  const config = cache.es.config
  const e1ToE2RelationSchema = config.schema[e1._type][e1ToE2Relation]
  const e2ToE1Relation = e1ToE2RelationSchema.inName
  const e2Schema = config.schema[e2._type]
  const unionInPathsForE2ToE1Relation = e2Schema[e2ToE1Relation] && e2Schema[e2ToE1Relation].unionIn //Ex. primaryLanguages in case of speaker
  if (!unionInPathsForE2ToE1Relation) {
    return Q()
  }
  return async.each(unionInPathsForE2ToE1Relation, (unionInPathForE2ToE1Relation) => {
    return suspendable(() => {
      let destEntitiesToUnionIn = await(
        utils.getEntitiesAtPath(cache, e2, _.dropRight(unionInPathForE2ToE1Relation, 1))
      )
      return async.each(destEntitiesToUnionIn, (destEntityToUnionIn) => {
        return utils.recalculateUnionInSibling(null, e2, e2ToE1Relation, destEntityToUnionIn, _.last(unionInPathForE2ToE1Relation), cache)
      })
    })()
  })

}

const updateSibling = (e1, e2, e1ToE2Relation, cache, config) => {
  return updateE2OnNewLinkWithE1(e2, e1, e1ToE2Relation, cache)
  .then(() => {
    return updateE2GraphOnNewLinkWithE1(e2, e1, e1ToE2Relation, cache)
  })
  .then(() => {
    const e1Schema = config.schema[e1._type]
    return async.each(_.keys(e1Schema), (field) => {
      const fieldSchema = e1Schema[field]

      /**if (fieldSchema.unionIn && _.includes(fieldSchema.unionIn, e1ToE2Relation)) {
        return utils.recalculateUnionInSibling(null, e1, field, e2, e1ToE2Relation, e2._id, cache)
      }**/

      if (fieldSchema.copyTo && _.includes(fieldSchema.copyTo, e1ToE2Relation)) {
        return utils.updateCopyToSibling(e1, e1ToE2Relation, e2._id, {set: {[field]: e1[field]}}, cache)
      }
    })
  })
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
