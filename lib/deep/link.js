var debug = require('debug')('crudLink')
var async = require('async-q')
var _ = require('lodash')
var Q = require('q')
var updater = require('../utils/update')
var configs = require('../../../configs')
var es = require('../es')

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

 * @params e1
 * @params e1ToE2Relation
 * @params e2Entities single or array of entities of possibly multiple types, with _type and _id fields
 *
 */
module.exports = (params, cachedEntities) => {
  var indexEntitiesAtEnd = false
  if (!cachedEntities) {
    cachedEntities = {}
    indexEntitiesAtEnd = true
  }
  return async.each(params.e2Entities, (e2) => {
    if (params.e1ToE2Relation) {
      return dualLink(params.e1, e2, params.e1ToE2Relation, cachedEntities)
    } else {
      return dualLink(e2, params.e1, params.e2ToE1Relation, cachedEntities)
    }
  })
  .then(() => {
    if (indexEntitiesAtEnd) {
      debug('About to index updated tree after linking')
      return async.each(_.values(cachedEntities), (updatedEntity) => {
        return es.index.agg({
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
}

const dualLink = (e1, e2, e1ToE2Relation, cachedEntities) => {
  var e1ToE2RelationDef = configs.schema[e1._type][e1ToE2Relation]
  if (!e1ToE2RelationDef) {
    throw new Error('Relation not found: EntityType: ' + e1._type + '. Relation: ' + e1ToE2Relation)
  }
  var e2ToE1Relation = e1ToE2RelationDef.revName

  return Q.all([
    makeLink(e1, e1ToE2Relation, e2, cachedEntities),
    makeLink(e2, e2ToE1Relation, e1, cachedEntities)]
  )
}

const makeLink = (e1, e1ToE2Relation, e2, cachedEntities) => {
  if (!e1ToE2Relation) {
    return Q()
  }
  return isAlreadyLinked(e1, e2, e1ToE2Relation, cachedEntities)
  .then((isAlreadyLinked) => {
    if (isAlreadyLinked) {//Don't need to link
      debug('makeLink. Is already linked. Stopping here', e1._type, e1._id, e1ToE2Relation, e2._id)
      return Q()
    } else {
      debug('makeLink: Adding new Link', e1._type, e1._id, e1ToE2Relation, e2._id)
    }
    var e1Schema = configs.schema[e1._type]
    var relationSchema = e1Schema[e1ToE2Relation]
    if (!relationSchema) {
      throw new Error('makeLink: ' + e1ToE2Relation + ' not found in ' + e1._type + ' schema' )
    }
    var linkOp = _.isArray(relationSchema.type) && 'addToSet' || 'set'
    //debug(e1._type, e1ToE2Relation, linkOp, JSON.stringify(_.pick(e2.fields, [e1ToE2Relation])))
    return updater({
      _type: e1._type,
      _id: e1._id,
      update: {
        [linkOp]: {
          [e1ToE2Relation]: e2._id
        }
      }
    }, cachedEntities)
    .then(() => {
      return updateSibling(e1, e1ToE2Relation, e2._id, cachedEntities)
    })
  })
}

const isAlreadyLinked = (e1, e2, e1ToE2Relation, cachedEntities) => {
  return updater.getEntity(cachedEntities, e1._id, e1._type)
  .then((e1Full) => {
    return _.includes(e1Full._source[e1ToE2Relation], e2._id)
  })
}

const updateSibling = (e1, e2, e1ToE2Relation, cachedEntities) => {
  var e1Schema = configs.schema[e1._type]
  return async.each(_.keys(e1Schema), (field) => {
    var fieldSchema = e1Schema[field]

    if (fieldSchema.unionIn && _.includes(fieldSchema.unionIn, e1ToE2Relation)) {
      return updater.recalculateUnionInSibling(field, null, e1, e1ToE2Relation, e2._id, cachedEntities)
    }

    if (fieldSchema.copyTo && _.includes(fieldSchema.copyTo, e1ToE2Relation)) {
      return updater.updateCopyToSibling(e1, e1ToE2Relation, e2._id, {set: {[field]: e1[field]}}, cachedEntities)
    }
  })
}

if (require.main === module) {
  module.exports({
    e1: {
      _type: 'file',
      _id: '1112'
    },
    e2Entities: [{
      _type: 'folder',
      _id: '3'
    }],
    e1ToE2Relation: 'folders'
  })
  .then(function(res) {
    debug('done', JSON.stringify(res))
  })
  .catch(debug)
}
