'use strict'

var debug = require('debug')('resolveJoins')
var _ = require('lodash')
var async = require('async-q')
var Q = require('q')

var fieldsToFetch = require('./fieldsToFetch')

/**
 *Joins the relationships of an entity based on a particular joinConfig
 *@param {Cache} cache
 *@param {[String]} langs - languages to be resolved. If not specified, gets the entire document with all languages. Default value is config.common.supportedLanguages
 *@param {String Or Object} joinConfigPathOrConfig - Either the path to load config from within the global config, or the loaded config itself for this join
 *@param {Object} entity - the doc whose joins have to be resolved
 *
 */
module.exports = function(cache, langs, joinConfigPathOrConfig, entity) {
  const es = cache.es
  const configs = es.config
  let joins
 
  if(_.isObject(joinConfigPathOrConfig)) {

    joins = joinConfigPathOrConfig
  } else {

    joins = _.get(configs.joins, joinConfigPathOrConfig.split('.').concat(entity._type))
  }
                                                                       if (!joins) {
    return Q(entity)
  }

  entity.fields = entity.fields || entity._source
  delete entity._source

  let schema = configs.schema[entity._type]
  return async.each(_.keys(joins), (toJoinFieldName) => {

    let fieldSchema = schema[toJoinFieldName]

    if (!fieldSchema.isRelationship) {
      return
    }

    let toJoinEntities = entity.fields[toJoinFieldName]
    if (!toJoinEntities) {
      return
    }
    entity.fields[toJoinFieldName] = []//Initialize as array. Later will convert to object for non array relationship

    const langs = langs && _.flatten([langs]) || cache.es.config.common.supportedLanguages
    return async.each(langs, (language) => {
    
      let fields = fieldsToFetch.forEntity(es, fieldSchema.to, joins[toJoinFieldName], language)
      //we will replace array of ids with their respective documents
      return async.each(_.flatten([toJoinEntities]), (toJoinEntity) => {
        //Do not join for already joined entities. THey will be objects with more properties than just {_id,own} tuple
        if (toJoinEntity.fields || toJoinEntity._source) {
          return //This is already joined
        }
        //This needs a join
        return require('./get')({
          _type: fieldSchema.to,
          _id: toJoinEntity._id,
          fields: fields, //fields to join for this entity
          joins: joins[toJoinEntity._type], //recursive joins
          lang: language
        }, cache)
      })
    })

    .then((toJoinDocs) => {
      replaceSourceWithFields(toJoinDocs)
      if (fieldSchema.cardinality === 'one') {
        toJoinDocs = toJoinDocs[0]
      }
      entity.fields[toJoinFieldName] = toJoinDocs
    })
  })
  .then(() => {
    return entity
  })
  .catch((err) => {
    debug(err)
  })
}
/**
 * Renames _source field to fields, in the docs' deep tree
 */
const replaceSourceWithFields = (schema, docs) => {
  if (!_.isArray(docs)) {
    docs = [docs]
  }

  docs.forEach((doc) => {
    doc.fields = doc._source || doc.fields
    delete doc._source
    const entitySchema = schema[doc._type]
    //Now iterate over all relationships
    _.each(entitySchema, (fieldSchema, field) => {
      //Go inside the related, nested objects and update them too
      if (fieldSchema.isRelationship && doc.fields[field]) {
        renameSourceToFields(doc.fields[field])
      }
    })
  })
}
