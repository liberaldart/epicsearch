'use strict'
const debug = require('debug')('crudLink')
const async = require('async-q')
const _ = require('lodash')
const Q = require('q')

const utils = require('./utils')
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

Index.prototype.execute = function(params, cachedEntities) {
  const es = this.es
  const config = this.es.config
  let indexEntitiesAtEnd = false
  if (!cachedEntities) {
    cachedEntities = {}
    indexEntitiesAtEnd = true
  }
  const entities = _.flatten([params.e2 || params.e2Entities])
  return async.each(entities, (e2) => {
    if (params.e1ToE2Relation) {
      return dualLink(params.e1, e2, params.e1ToE2Relation, cachedEntities, config, es)
    } else {
      return dualLink(e2, params.e1, params.e2ToE1Relation, cachedEntities, configi, es)
    }
  })
  .then(() => {
    if (indexEntitiesAtEnd) {
      debug('About to index updated tree after linking')
      return async.each(_.values(cachedEntities), (updatedEntity) => {
        return es.index.collect({
          index: updatedEntity._index,
          type: updatedEntity._type,
          id: updatedEntity._id,
          body: updatedEntity._source
        })
      })
      .then(() => {
        return cachedEntities
      })
    }
  })
  .catch((err) => {
    debug(err, err.stack)
    throw err
  })
}

const dualLink = (e1, e2, e1ToE2Relation, cachedEntities, config, es) => {
  const e1ToE2RelationDef = config.schema[e1._type][e1ToE2Relation]
  if (!e1ToE2RelationDef) {
    throw new Error('Relation not found: EntityType: ' + e1._type + '. Relation: ' + e1ToE2Relation)
  }
  const e2ToE1Relation = e1ToE2RelationDef.inName

  return Q.all([
    makeLink(e1, e1ToE2Relation, e2, cachedEntities, config, es),
    makeLink(e2, e2ToE1Relation, e1, cachedEntities, config, es)]
  )
}

const makeLink = (e1, e1ToE2Relation, e2, cachedEntities, config, es) => {
  debug(e1, e2, e1ToE2Relation)
  if (!e1ToE2Relation) {
    return Q()
  }
  return isAlreadyLinked(e1, e2, e1ToE2Relation, cachedEntities, es)
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
    return es.deep.update({
      _type: e1._type,
      _id: e1._id,
      update: {
        [linkOp]: {
          [e1ToE2Relation]: e2._id
        }
      }
    }, cachedEntities)
    .then(() => {
      return updateSibling(e1, e1ToE2Relation, e2._id, cachedEntities, config, es)
    })
  })
}

const isAlreadyLinked = (e1, e2, e1ToE2Relation, cachedEntities, es) => {
  return utils.getEntity(cachedEntities, e1._id, e1._type, es)
  .then((e1Full) => {
    return _.includes(e1Full._source[e1ToE2Relation], e2._id)
  })
}

const updateSibling = (e1, e2, e1ToE2Relation, cachedEntities, config, es) => {
  const e1Schema = config.schema[e1._type]
  return async.each(_.keys(e1Schema), (field) => {
    const fieldSchema = e1Schema[field]

    if (fieldSchema.unionIn && _.includes(fieldSchema.unionIn, e1ToE2Relation)) {
      return es.deep.update.recalculateUnionInSibling(field, null, e1, e1ToE2Relation, e2._id, cachedEntities)
    }

    if (fieldSchema.copyTo && _.includes(fieldSchema.copyTo, e1ToE2Relation)) {
      return es.deep.update.updateCopyToSibling(e1, e1ToE2Relation, e2._id, {set: {[field]: e1[field]}}, cachedEntities)
    }
  })
}

module.exports = Index

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('../../newConfig')
  es.deep.link({
    e1: {
      _type: 'a',
      _id: '1'
    },
    e2: {
      _type: 'b',
      _id: '1'
    },
    e1ToE2Relation: 'relationAB'
  })
  .then(function(res) {
    debug('done', JSON.stringify(res))
  })
  .catch(debug)
}