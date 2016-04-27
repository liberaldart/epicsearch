'use strict'

var debug = require('debug')('resolveJoins')
var _ = require('lodash')
var async = require('async-q')
var Q = require('q')

var es = require('../es')
var configs = require('../../../configs')
var fieldsToFetch = require('./fieldsToFetch')

module.exports = function(esDoc, lang, context, joins) {

  joins = joins || _.get(configs, context)[esDoc._type].joins
  if (!joins) {
    return Q(esDoc)
  }
  esDoc.fields = esDoc.fields || esDoc._source
  delete esDoc._source

  let schema = configs['schema'][esDoc._type]
  return async.each(joins, (joinField) => {
    let config = _.get(configs, context)
    let toJoinFieldName = joinField.fieldName
    let fieldSchema = schema[toJoinFieldName]

    let toJoinIds = esDoc.fields[toJoinFieldName]
    if (!toJoinIds) {
      return Q()
    }
    esDoc.fields[toJoinFieldName] = []
    let fields = fieldsToFetch.forEntity(fieldSchema.type, joinField, lang)
    //we will replace array of ids with their respective documents
    return async.each(_.flatten([toJoinIds]), (id) => {
      return require('./read')({
        _type: fieldSchema.type,
        _id: id,
        fields: fields,
        joins: joinField.joins,
        lang: lang
      })
    })
    .then((toJoinDocs) => {
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
