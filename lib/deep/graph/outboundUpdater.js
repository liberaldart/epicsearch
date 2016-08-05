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
  //THese joinInfos are of the form [{path: [String], joinAtPath: [String]}] where joinAtPath contains the path leading to the joined field within the destEntity

  return async.each(updatedFieldJoinIns, (joinInInfo) => {
    return utils.getEntitiesAtPath(cache, updatedEntity, joinInInfo.path)
    .then((toUpdateDestEntities) => {
      if (!toUpdateDestEntities || _.isEmpty(toUpdateDestEntities)) {
        return
      }
      if (!_.isArray(toUpdateDestEntities)) {
        toUpdateDestEntities = [toUpdateDestEntities]
      }
      //debug('recalculateUnionInGraph: will update union at path', unionInPath, 'sourceEntityNew', sourceEntityNew._source, 'sourceEntityOld._source', sourceEntityOld, 'to update entities', JSON.stringify(toUpdateDestEntities))
      return async.each(toUpdateDestEntities, (toUpdateDestEntity) => {
        const destEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields
        //Based on the first relation of joinInInfo.joinAtPath, get the top object or array within which join data is to be updated for this field 
        const destRelationName = _.first(joinInInfo.joinAtPath)

        //Locate the updatedEntity within destEntityBody[destRelationName]
        let udpatedEntityJoinedVersion
        if (cache.es.config.schema[toUpdateDestEntity._type][destRelationName].cardinality === 'many') {

          udpatedEntityJoinedVersion = _.find(destEntityBody[destRelationName], {_id: updatedEntity._id})

        } else {

          udpatedEntityJoinedVersion = destEntityBody[destRelationName]
        }
        if (!udpatedEntityJoinedVersion) {
          throw new Error('did not find joined doc. Updated entity', updatedEntity, 'updated field', updatedField, 'toUpdateDestEntity', toUpdateDestEntity, 'joinInfo', joinInfo)
        }
        //Now make the udpate in dest Entity
        updater({
          doc: udpatedEntityJoinedVersion.fields, 
          update: fieldUpdate,
          force: true
        })
        cache.markDirtyEntity(toUpdateDestEntity)
      })
    })
  })
}


/**
 * The sourceEntity has been updated to sourceEntityNew by applying an update on a field. Update the nodes connected to sourceEntity and having fields which unionFrom this particular field of sourceEntity. 
 * @param {Object} cache
 * @param {Object} sourceEntity
 * @param {Object} sourceEntityNew
 * @param {String} updatedField
 */
const recalculateUnionInDependentGraph = (cache, sourceEntityOld, sourceEntityNew, updatedField) => {
  const entitySchema = cache.es.config.schema[sourceEntityNew._type]
  const updatedFieldSchema = entitySchema[updatedField]
  const unionInPaths = _([updatedFieldSchema.unionIn]).flatten().compact().value()

  return async.each(unionInPaths, (unionInPath) => {
    return utils.getEntitiesAtPath(cache, sourceEntityNew, _.dropRight(unionInPath, 1))
    .then((toUpdateDestEntities) => {
      if (!toUpdateDestEntities) {
        return
      }
      if (_.isArray(toUpdateDestEntities)) {
        if (!_.size(_.compact(toUpdateDestEntities))) {
          return
        }
      } else { //Make it an array
        toUpdateDestEntities = [toUpdateDestEntities]
      }
      //debug('recalculateUnionInGraph: will update union at path', unionInPath, 'sourceEntityNew', sourceEntityNew._source, 'sourceEntityOld._source', sourceEntityOld, 'to update entities', JSON.stringify(toUpdateDestEntities))
      return async.each(toUpdateDestEntities, (toUpdateDestEntity) => {
        const destFieldToUpdate = _.last(unionInPath)
        return utils.recalculateUnionInSibling(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destFieldToUpdate, cache)
      })
      //.then(() => {
      //  debug(updatedField, sourceEntityNew)
      //})
    })
  })
}

module.exports.recalculateUnionInDependentGraph = recalculateUnionInDependentGraph
module.exports.recalculateJoinsInDependentGraph = recalculateJoinsInDependentGraph
