'use strict'

var debug = require('debug')('resolveJoins')
var _ = require('lodash')
var async = require('async-q')
var Q = require('q')

var fieldsToFetch = require('./fieldsToFetch')

module.exports = function(cache, esDoc, lang, context, joins) {
  const es = cache.es
  const configs = es.config
  joins = joins || _.get(configs, [context, esDoc._type, 'joins'])
  if (!joins) {
    return Q(esDoc)
  }
  esDoc.fields = esDoc.fields || esDoc._source
  delete esDoc._source

  let schema = configs['schema'][esDoc._type]
  return async.each(joins, (join) => {
    let config = _.get(configs, context)
    let toJoinFieldName = join.fieldName
    let fieldSchema = schema[toJoinFieldName]

    let toJoinEntities = esDoc.fields[toJoinFieldName]
    if (!toJoinEntities) {
      return Q()
    }
    esDoc.fields[toJoinFieldName] = []//Initialize as array. Later will convert to object for non array relationship
    let fields = fieldsToFetch.forEntity(es, fieldSchema.type, join, lang)
    //we will replace array of ids with their respective documents
    return async.each(_.flatten([toJoinEntities]), (toJoinEntity) => {
      return require('./get')({
        _type: fieldSchema.type,
        _id: toJoinEntity._id,
        fields: fields, //fields to join for this entity
        joins: join.joins, //recursive joins
        lang: lang
      }, cache)
    })
    .then((toJoinDocs) => {
      renameSourceToJoin(toJoinDocs)
      if (!_.isArray(fieldSchema.type)) {
        toJoinDocs = toJoinDocs[0]
      }
      esDoc.fields[toJoinFieldName] = toJoinDocs
    })
  })
  .then(() => {
    return esDoc
  })
  .catch((err) => {
    debug(err)
  })
}

const renameSourceToJoin = (schema, docs) => {
  docs.forEach((doc) => {
    doc.join = doc._source || doc.fields
    delete doc._source
    delete doc.fields
    const entitySchema = schema[doc._type]
    _.each(entitySchema, (fieldSchema, field) => {
      if (fieldSchema.isRelationship && doc.join[field]) {
        const innerJoinedDocs = [doc.join[field]]
        renameSourceToJoin(innerJoinedDocs)
      }
    })
  })
}
