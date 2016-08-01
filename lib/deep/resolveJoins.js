'use strict'

var debug = require('debug')('epicsearch:deep/resolveJoins')
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
  const configs = cache.es.config
  let joinsForEntity
 
  if(_.isObject(joinConfigPathOrConfig)) {

    joinsForEntity = joinConfigPathOrConfig
  } else {

    joinsForEntity = _.get(configs.joins, joinConfigPathOrConfig.split('.').concat(entity._type))
  }
  if (!joinsForEntity) {
    return Q(entity)
  }

  //debug('to do joins for', entity)
  const entityBody = entity.fields || entity._source

  const schema = configs.schema[entity._type]
  return async.each(_.keys(joinsForEntity), (toJoinFieldName) => {
    const fieldSchema = schema[toJoinFieldName]

    if (!fieldSchema.isRelationship) {
      return
    }
    
    let toJoinEntities = entityBody[toJoinFieldName]
    toJoinEntities = _.filter(entityBody[toJoinFieldName], (toJoinEntity) => {
      toJoinEntity._type = fieldSchema.to

      return !toJoinEntity.fields && !toJoinEntity._source //A non joined entity will have _id:"idStr", type and own: true at max. But not fields or _source
    })
    if (!toJoinEntities) {
      return
    }
    entityBody[toJoinFieldName] = []//Initialize as array. Later will convert to object for non array relationship

    langs = langs && _.flatten([langs]) || cache.es.config.common.supportedLanguages

    let fields = fieldsToFetch.forEntity(cache.es, fieldSchema.to, joinsForEntity[toJoinFieldName], langs)

    //we will replace array of ids with their respective documents
    return async.map(_.flatten([toJoinEntities]), (toJoinEntity) => {
      //Do not join for already joined entities. THey will be objects with more properties than just {_id,own} tuple
      if (toJoinEntity.fields || toJoinEntity._source) {
        return toJoinEntity //This is already joined
      }
      //debug('before deep get', toJoinEntity._type, cache.data[toJoinEntity._id+fieldSchema.to])
      //This needs a join
      return cache.es.deep.get({//This get will also save the docs in cache
        _type: fieldSchema.to,
        _id: toJoinEntity._id,
        fields: fields, //fields to join for this entity
        joins: joinsForEntity[toJoinEntity._type], //recursive joins
        langs: langs 
      }, cache)
    })
    .then((toJoinDocs) => {
      //This needs a join
      toJoinDocs =
        _(toJoinDocs)
        .flatten()
        .compact()
        .value()

      if (!toJoinDocs.length) {
        return
      }

      //Avoid to edit original es returned docs states stored in cache, for they represent the actual docs. Can sanitizie/modify a copy of these docs instead
      toJoinDocs = toJoinDocs.map((toJoinDoc) => _.cloneDeep(toJoinDoc))

      purgeUnwantedProperties(toJoinDocs)
      debug('toJoinDocs after sanitize', JSON.stringify(toJoinDocs), joinsForEntity[toJoinFieldName])

      if (fieldSchema.cardinality === 'one') {
        toJoinDocs = toJoinDocs[0]
      }
      entityBody[toJoinFieldName] = toJoinDocs
    })
    .then(() => {
      return entity
    })
  })
}

const FIELDS_TO_PURGE = ['_source', '_index', '_type', 'found']
const purgeUnwantedProperties = (docs) => {
  return docs.forEach((doc) =>
    FIELDS_TO_PURGE.forEach((field) => delete doc[field])
  )
}
/**
//FOr lodash _.merge
const deepMergeCustomizer = (existingValue, latestValue, field, existingDoc, toMergeDoc) => {

  if (!existingValue) {
    return latestValue
  }

  if (!latestValue) {
    return existingValue
  }

  //If both values exist
  if (_.isObject(latestValue)) {

    _.merge(existingValue, latestValue, mergeCustomizer)

  } else if (_.isArray(latestValue)) {

    latestValue.forEach((item) => {

      if (_.isObject(item)) { //Must be an entity
        existingItem = _.find(existingValue, {_id: item._id})

        if (existingItem) {
          _.merge(existingItem, item, mergeCustomizer)

        } else {
          existingValue.push(item)
        }
      }
    })
  }

  return existingValue
}
**/
