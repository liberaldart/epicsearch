const Q = require('q')
const async = require('async-q')
const _ = require('lodash')
const deepMerge = require('deepmerge')
const debug = require('debug')('epicsearch:deep/graph/unionCalculator')

const utils = require('../utils')
const traverser = require('./traverser')

module.exports.recalculateUnionInSibling = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField, cache) => {
  /**if (!cache.es.config.schema[sourceEntityNew._type][updatedField].isRelationship) {
    return Q()
  }**/

  const newFieldValue = (sourceEntityNew._source || sourceEntityNew.fields)[updatedField]
  const oldFieldValue = sourceEntityOld && (sourceEntityOld._source || sourceEntityOld.fields)[updatedField]


  if (_.isEmpty(newFieldValue) && _.isEmpty(oldFieldValue)) {
    return Q()
  }

  debug('recalculateUnionInSibling', sourceEntityNew._type, sourceEntityNew._id, 'updated field ', updatedField, 'new value', newFieldValue, 'old value', oldFieldValue, 'to update entity', JSON.stringify(toUpdateDestEntity))

  //Initialize meta for updatedField if necessary
  const toUpdateDestEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields
  toUpdateDestEntityBody.meta = toUpdateDestEntityBody.meta || {}
  toUpdateDestEntityBody.meta[destEntityField] = toUpdateDestEntityBody.meta[destEntityField] || {}

  const newLinks = 
    handleIncrements(cache, sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  const removedLinks = 
    handleDecrements(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField)

  return updateUnionsInDestEntity(cache, toUpdateDestEntity, destEntityField)
}

const handleIncrements = (cache, sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {

  let increments

  if (cache.es.config.schema[sourceEntityNew._type][updatedField].isRelationship) {
    increments = getIncrementsForRelation(sourceEntityOld, sourceEntityNew, updatedField)  
  } else {
  
    increments = getIncrementsForField(sourceEntityOld, sourceEntityNew, updatedField)  
  }
  
  const toUpdateDestEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields
    
 // debug('dddddddddddddddddddddddd', increments, sourceEntityNew, sourceEntityOld)
  for (let value of increments) {//Update related entity accordingly
    toUpdateDestEntityBody.meta[destEntityField][value] = toUpdateDestEntityBody.meta[destEntityField][value] || 0
    toUpdateDestEntityBody.meta[destEntityField][value] += 1
  }
  return increments
}

const getIncrementsForRelation = (sourceEntityOld, sourceEntityNew, updatedField) => {
  //Increment the count in meta of added values to the sourceEntityOld
  let newIds = _([(sourceEntityNew._source || sourceEntityNew.fields)[updatedField]])
    .flatten()
    .compact()
    .map('_id')
    .value()

  let increments = newIds

  if (sourceEntityOld) {
    let oldIds = _([(sourceEntityOld._source || sourceEntityOld.fields)[updatedField]])
      .flatten()
      .compact()
      .map('_id')
      .value()

    increments = _.difference(newIds, oldIds)
  }

  return increments

}

const getIncrementsForField = (sourceEntityOld, sourceEntityNew, updatedField) => {
  
  let newValues = _([(sourceEntityNew._source || sourceEntityNew.fields)[updatedField]])
    .flatten()
    .compact()
    .value()

  let increments = newValues

  if (sourceEntityOld) {
    let oldValues = _([(sourceEntityOld._source || sourceEntityOld.fields)[updatedField]])
      .flatten()
      .compact()
      .value()

    increments = _.difference(newValues, oldValues)
  }

  return increments
}


const handleDecrements = (sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destEntityField) => {

  if (sourceEntityOld) {
    let newIds = _([(sourceEntityNew._source || sourceEntityNew.fields)[updatedField]])
      .flatten()
      .compact()
      .map('_id')
      .value()
    let oldIds = _([sourceEntityOld && (sourceEntityOld._source || sourceEntityOld.fields)[updatedField]])
      .flatten()
      .compact()
      .map('_id')
      .value()
      //Decrement counts in meta for the removed values from the sourceEntityOld
    const decrements = _.difference(oldIds, newIds)
    //debug(sourceEntityNew._type, 'found decrements', decrements, sourceEntityNew._source[updatedField], sourceEntityOld._source[updatedField])
    
    const toUpdateDestEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields

    decrements.forEach((id) => {//Update related entity accordingly
      const referenceCounts = toUpdateDestEntityBody.meta[destEntityField]
      //Reduce count by 1, and if it has become zero remove this key
      if (! --referenceCounts[id]) {
        delete referenceCounts[id]
        if (_.isEmpty(referenceCounts)) { //If no more references are left, delete the empty object too
          delete toUpdateDestEntityBody.meta[destEntityField]
        }
      }
    })
    return decrements
  }
}

const updateUnionsInDestEntity = (cache, toUpdateDestEntity, destEntityField, increments, decrements) => {

  const toUpdateDestEntityBody = toUpdateDestEntity._source || toUpdateDestEntity.fields
  const existingValues = toUpdateDestEntityBody[destEntityField]
  const isRelation = cache.es.config.schema[toUpdateDestEntity._type][destEntityField].isRelationship

  //Now calculate new field value in toUpdateDestEntity through union of the reference counts and ownership
  const valuesWithPositiveCounts = _.transform(toUpdateDestEntityBody.meta[destEntityField], (result, count, value) => {

    if (!_.isUndefined(count) && count > 0) {
      //Push joined object if it exists, or push just the _id, to be later joined
      if (isRelation) {
        result.push(_.find(existingValues, {_id: value}) || {_id: value})
      } else {
        result.push(value)
      }
    }
  }, [])

  let newFieldValue = 
    _(isRelation && _.filter(existingValues, {own: true}) || undefined)
    .union(valuesWithPositiveCounts)
    .value()

  if (_.isEmpty(newFieldValue) && _.isEmpty(existingValues)) {
    return Q()
  }

  //For new links, resolveJoins and push
  //For removed links, call pull
  let updateInstruction
  if (_.isEmpty(newFieldValue)) {
    updateInstruction = {unset: {_path: destEntityField}}
  } else {
    updateInstruction = {set: {[destEntityField]: newFieldValue}} 
  }
  //debug('about to deep update unionIn', JSON.stringify(toUpdateDestEntity), 'new field value', JSON.stringify(newFieldValue), ' for field', destEntityField, 'updateInstruction', updateInstruction)
  //Update the related entity and its tree with the new field value
  return cache.es.deep.update(
    {
      _id: toUpdateDestEntity._id,
      _type: toUpdateDestEntity._type,
      update: updateInstruction 
    },
    cache
  )
  .then((res) => {
    cache.markDirtyEntity(toUpdateDestEntity)

    const cachedDestEntity = cache.get(toUpdateDestEntity._id + toUpdateDestEntity._type)
    const cachedDestEntityBody = cachedDestEntity._source || cachedDestEntity.fields
    cachedDestEntityBody.meta = deepMerge(cachedDestEntityBody.meta || {}, toUpdateDestEntityBody.meta)
    return res
  })
}

/**
 * The sourceEntity has been updated to sourceEntityNew by applying an update on a field. Update the nodes connected to sourceEntity and having fields which unionFrom this particular field of sourceEntity. 
 * @param {Object} cache
 * @param {Object} sourceEntity
 * @param {Object} sourceEntityNew
 * @param {String} updatedField
 */
module.exports.recalculateUnionInDependentGraph = (cache, sourceEntityOld, sourceEntityNew, updatedField) => {
  const entitySchema = cache.es.config.schema[sourceEntityNew._type]
  const updatedFieldSchema = entitySchema[updatedField]
  const unionInPaths = _([updatedFieldSchema['unionIn']]).flatten().compact().value()

  return async.each(unionInPaths, (unionInPath) => {
    return utils.getEntitiesAtRelationPath(cache, sourceEntityNew, _.dropRight(unionInPath, 1))
    .then((toUpdateDestEntities) => {
      if (!toUpdateDestEntities.entities) {
        return
      }
      if (_.isArray(toUpdateDestEntities.entities)) {
        if (!_.size(_.compact(toUpdateDestEntities.entities))) {
          return
        }
      } else { //Make it an array
        toUpdateDestEntities.entities = [toUpdateDestEntities.entities]
      }
      //debug('recalculateUnionInGraph: will update union at path', unionInPath, 'sourceEntityNew', sourceEntityNew._source, 'sourceEntityOld._source', sourceEntityOld, 'to update entities', JSON.stringify(toUpdateDestEntities))
      return async.each(toUpdateDestEntities.entities, (toUpdateDestEntity) => {
        const destFieldToUpdate = _.last(unionInPath)
        return Q() //TODO fix this
        //return module.exports.recalculateUnionInSibling(sourceEntityOld, sourceEntityNew, updatedField, toUpdateDestEntity, destFieldToUpdate, cache)
      })
    })
  })
}

