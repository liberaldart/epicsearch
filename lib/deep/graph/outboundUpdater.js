'use strict'

const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const debug = require('debug')('epicsearch:deep/outGraphUpdate')

const utils = require('../utils')

const recalculateJoinsInDependentGraph = (cache, updatedEntity, updatedField, fieldUpdate) => {
  const invertedJoinEntityConfig = cache.es.config.invertedJoins.index[updatedEntity._type] //index is the default config used for index time joins in graph

  if (!invertedJoinEntityConfig) {
    return
  }

  let updatedFieldJoinIns = invertedJoinEntityConfig[updatedField] //Inverted join info is stored as an array per field
  if (!updatedFieldJoinIns) {
    return
  }

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
        return joinTheValue(cache, toUpdateDestEntity, updatedEntity, updatedField, toUpdateDestEntities.idToEdgePathMap[toUpdateDestEntity._id])
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
  sourceEntityJoinedVersion.fields[sourceField] = (sourceEntity.fields || sourceEntity._source)[sourceField]

  cache.markDirtyEntity(destEntity)
}



module.exports.recalculateJoinsInDependentGraph = recalculateJoinsInDependentGraph
module.exports.joinTheValue = joinTheValue
