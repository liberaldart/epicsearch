'use strict'

var debug = require('debug')('epicsearch:deep/graph/joinCalculator')
var _ = require('lodash')
var async = require('async-q')
var Q = require('q')

const utils = require('../utils')
var fieldsToFetch = require('../fieldsToFetch')

/**
 *Joins the relationships of an entity based on a particular joinConfig
 *@param {Cache} cache
 *@param {[String]} langs - languages to be resolved. If not specified, gets the entire document with all languages. Default value is config.common.supportedLanguages
 *@param {String Or Object} joinConfigPathOrConfig - Either the path to load config from within the global config, or the loaded config itself for this join
 *@param {Object} entity - the doc whose joins have to be resolved
 *@param {Object} compulsoryJoins - Of the form, {<relationName>: <entity || [entity]>}. If specified these, particular entities are joined even if they are already joined previously
 *
 */
module.exports.resolveForEntity = function(cache, langs, joinConfigPathOrConfig, entity, compulsoryJoins) {
  const configs = cache.es.config
  let joinsForEntity
 
  if(_.isObject(joinConfigPathOrConfig)) {

    joinsForEntity = joinConfigPathOrConfig
  } else {

    joinConfigPathOrConfig = joinConfigPathOrConfig || 'index'
    joinConfigPathOrConfig = joinConfigPathOrConfig.split('.').concat(entity._type)
    joinsForEntity = _.get(configs.joins, joinConfigPathOrConfig)
  }
  if (!joinsForEntity) {
    //debug(entity._type, joinConfigPathOrConfig)
    return Q(entity)
  }

  const entityBody = entity.fields || entity._source
  if (!entityBody) {
    return Q(entity)
  }
  const schema = configs.schema[entity._type]

  return async.each(_.keys(compulsoryJoins || joinsForEntity), (toJoinFieldName) => {

    const fieldSchema = schema[toJoinFieldName]
    
    
    if (!fieldSchema.isRelationship || !joinsForEntity[toJoinFieldName]) {
     
      //debug('Ruturunug-dddddddddddddddddddd', toJoinFieldName, entity._type)
      return
    }

    //if (entityBody[toJoinFieldName]) 
      //debug(entity._type, toJoinFieldName, entityBody[toJoinFieldName])
    let toJoinEntities = 
      _([entityBody[toJoinFieldName]])
      .flatten()
      .compact()
      .each((toJoinEntity) => {
        toJoinEntity._type = fieldSchema.to
      })
      .value()

    if (_.isEmpty(toJoinEntities)) {
      return
    }
    //debug('about to resolve joins for ' + toJoinFieldName + ' in ' + entity._type, 'entities', toJoinEntities, 'compulsory joins', compulsoryJoins, 'join for entity', joinsForEntity, 'entity', entity)

    entityBody[toJoinFieldName] = []//Initialize as array. Later will convert to object for non array relationship

    langs = langs && _.flatten([langs]) || cache.es.config.common.supportedLanguages

    let fields = fieldsToFetch.forEntity(cache.es, fieldSchema.to, joinsForEntity[toJoinFieldName], langs, true)

    //we will replace array of ids with their respective documents
    return async.map(toJoinEntities, (toJoinEntity) => {
      //Do not join for already joined entities. THey will be objects with more properties than just {_id,own} tuple
      if ((toJoinEntity.fields || toJoinEntity._source) && !_.find(_.flatten([compulsoryJoins[toJoinFieldName]]), {_id: toJoinEntity._id})) {
        return toJoinEntity //This is already joined AND is NOT to be compulsorily joined
      }
      //debug('before deep get', toJoinEntity._type, cache.data[toJoinEntity._id+fieldSchema.to])
      //This needs a join
      const getParams = {
        _type: fieldSchema.to,
        _id: toJoinEntity._id,
        fields: fields, //fields to join for this entity
        joins: joinsForEntity[toJoinFieldName], //recursive joins
        langs: langs 
      }
      return cache.es.deep.get(getParams, cache)//This get will also save the docs in cache
      .then((toJoinDoc) => {
        //debug(toJoinDoc, getParams, cache.get(toJoinDoc._id+toJoinDoc._type))
        //debug('ddddddddddddddddddddd', toJoinFieldName, JSON.stringify(toJoinDoc), entity._type, joinsForEntity[toJoinFieldName])

        if (toJoinEntity.own) {
          toJoinDoc.own = true
        }
        return toJoinDoc
      })
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
      toJoinDocs = toJoinDocs.map((toJoinDoc) => {
        const copy = _.cloneDeep(toJoinDoc)
        copy.fields = copy.fields || copy._source
        return copy
      })

      //debug('resolved joins for ' + toJoinFieldName + ' in ' + entity._type, 'joined entity docs', JSON.stringify(toJoinDocs), 'to join entities before resolution', JSON.stringify(toJoinEntities)) 
      purgeUnwantedProperties(toJoinDocs)
      //debug('toJoinDocs after sanitize', entity, JSON.stringify(toJoinDocs), joinsForEntity[toJoinFieldName])

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

const FIELDS_TO_PURGE = ['_source', '_index', '_type', 'found', 'isUpdated']
const purgeUnwantedProperties = (docs) => {
  return docs.forEach((doc) =>
    FIELDS_TO_PURGE.forEach((field) => delete doc[field])
  )
}

module.exports.recalculateJoinsInDependentGraph = (cache, updatedEntity, updatedField, fieldUpdate) => {
  const invertedJoinEntityConfig = cache.es.config.invertedJoins.index[updatedEntity._type] //index is the default config used for index time joins in graph

  if (!invertedJoinEntityConfig) {
    return
  }

  let updatedFieldJoinIns = invertedJoinEntityConfig[updatedField] //Inverted join info is stored as an array per field
  if (!updatedFieldJoinIns) {
    return
  }
  //debug('dddddddd', updatedEntity._type, updatedField, fieldUpdate, updatedFieldJoinIns)

  updatedFieldJoinIns = _.isArray(updatedFieldJoinIns) && updatedFieldJoinIns || [updatedFieldJoinIns]
  //THese joinInfos are of the form [{path: [String], joinAtPath: [String]}] where path is path from source to dest entity and joinAtPath is the path, in the destEntity, leading to the entity containing copy of value of the joined field

  return async.each(updatedFieldJoinIns, (joinInInfo) => {
    return utils.getEntitiesAtRelationPath(cache, updatedEntity, joinInInfo.path)
    .then((toUpdateDestEntities) => {
      if (!toUpdateDestEntities.entities || _.isEmpty(toUpdateDestEntities.entities)) {
        return
      }
      if (!_.isArray(toUpdateDestEntities.entities)) {
        toUpdateDestEntities.entities = [toUpdateDestEntities.entities]
      }
      return async.each(toUpdateDestEntities.entities, (toUpdateDestEntity) => {
        const destEntityToUpdatedEntityPath = utils.reverseEdgePath(cache, updatedEntity, toUpdateDestEntities.idToEdgePathMap[toUpdateDestEntity._id], toUpdateDestEntity)
        return joinTheValue(cache, toUpdateDestEntity, updatedEntity, updatedField, destEntityToUpdatedEntityPath)
      })
    })
  })
}

const getNested = (cache, entity, edgePath, forceCreate) => {
  const entityBody = entity._source || entity.fields
  let edgePathFirstEntities = entityBody[edgePath[0]]

  if (!edgePathFirstEntities) { //Initialize
    if (!forceCreate) {
      return
    }
    if (cache.es.config.schema[entity._type][edgePath[0]].cardinality === 'one') {
      entityBody[edgePath[0]] = edgePathFirstEntities = {} 
    } else { //many
      entityBody[edgePath[0]] = edgePathFirstEntities = []
    }
  }
  let edgePathFirstEntity
  //Now go nested deep
  if (_.isArray(edgePathFirstEntities)) {

    const edgePathFirstEntityInit = {_id: edgePath[1]}
    edgePathFirstEntity = _.findWhere(edgePathFirstEntities, edgePathFirstEntityInit)

    if (!edgePathFirstEntity) {
      if (!forceCreate) {
        return
      }
      edgePathFirstEntityInit.fields = {}
      edgePathFirstEntities.push(edgePathFirstEntityInit)
      edgePathFirstEntity = edgePathFirstEntityInit
    }

  } else { //First relation is a single cardinality relation
    if (edgePathFirstEntities._id === edgePath[1]) {
      edgePathFirstEntity = edgePathFirstEntities  
    } else {
      if (!forceCreate) {
        return
      } else {
        entityBody[edgePath[0]]._id = edgePath[1]
        entityBody[edgePath[0]].fields = {} 
        edgePathFirstEntity = entityBody[edgePath[0]]
      }
    }
  }

  if (edgePath.length > 2) {
    return getNested(cache, edgePathFirstEntity, _.drop(edgePath, 2), forceCreate)
  } else {
    return edgePathFirstEntity
  }
}

const joinTheValue = (cache, destEntity, sourceEntity, sourceField, edgePathToSource) => {

  const destEntityBody = destEntity._source || destEntity.fields
  //Based on the first relation of joinInInfo.joinAtPath, get the top object or array within which join data is to be updated for this field 

  //Locate the updatedEntity within destEntityBody[destRelationName]
  let sourceEntityJoinedVersion = getNested(cache, destEntity, edgePathToSource, true)
  if (!sourceEntityJoinedVersion) {
    throw new Error('did not find joined doc. Updated entity', updatedEntity, 'updated field', updatedField, 'toUpdateDestEntity', toUpdateDestEntity, 'joinInfo', joinInfo)
  }
  
  //Now make the udpate in dest Entity
  sourceEntityJoinedVersion.fields = sourceEntityJoinedVersion.fields || {} 
  
  const sourceEntityBody = sourceEntity.fields || sourceEntity._source
  debug(cache.es.config.schema[sourceEntity._type][sourceField])
  if (cache.es.config.schema[sourceEntity._type][sourceField].multiLingual) {
    cache.es.config.common.supportedLanguages.forEach((lang) => {
      const sourceLangData = _.get(sourceEntityBody, [lang, sourceField])
      sourceLangData && _.set(sourceEntityJoinedVersion.fields, [lang, sourceField], sourceLangData)
      
    })
  
  } else {
    sourceEntityJoinedVersion.fields[sourceField] = sourceEntityBody[sourceField]
  
  }

  cache.markDirtyEntity(destEntity)
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
