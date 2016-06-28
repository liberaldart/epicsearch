'use strict'
const debug = require('debug')('update')
const updater = require('js-object-updater')
const async = require('async-q')
const Q = require('q')
const _ = require('lodash')

const utils = require('./utils')

const DeepUpdater = class DeepUpdater {
  constructor(es) {
    this.es = es
  }

/**
 *@param {entities: [], update:{}} for multiple entities update or {_id: '', _type: '', update: {}} for single entity
 */
  execute(params, cache) {
    let flushCacheAtEnd = false
    if (!cache) {
      flushCacheAtEnd = true
      cache = new Cache(this.es)
    }
    params.entities = params.entities || [{_id: params._id, _type: params._type}]
    const index = this
    return async.each(params.entities, function(e) {
      return index.updateEntityAndSiblings(
        {
          _id: e._id,
          _type: e._type,
          update: params.update,
        },
        cache
      )
    })
    .then(() => {
      if (flushCacheAtEnd) {
        return cache.flush()
      }
    })
  }

  updateEntityAndSiblings(params, cache) {
    params._type = params.type || params._type
    params._id = params._id || params.id
    const deepUpdater = this
    return utils.getEntity(cache, params._id, params._type, deepUpdater.es)
    .then((entity) => {
      const updatedEntity = _.cloneDeep(entity)
      updater({
        doc: updatedEntity._source,
        update: params.update,
        force: true
      })
      cache.setEntity(updatedEntity)//Replace entity in cache with updatedEntity
      if (!_.isEqual(entity, updatedEntity)) {
        cache.markDirtyEntity(updatedEntity)
      }
      //debug('updateEntityAndSiblings', JSON.stringify(params))
      //debug('just set in cache', updatedEntity._source)
      return DeepUpdater.doAllUpdates(entity, updatedEntity, params, cache)
    })
  }

  static doAllUpdates(entity, updatedEntity, params, cache) {
    const entitySchema = cache.es.config.schema[updatedEntity._type]
    return async.each(_.keys(entitySchema), (field) => {
      const fieldUpdate = DeepUpdater.pluckUpdatesForField(field, params.update)
      if (entity && _.isEmpty(fieldUpdate)) {//If this field is being updated, and updatedEntity represents a new entity being created
        return Q()
      }
      //debug('percolate Updates', updatedEntity._type, updatedEntity._id, 'field', field, 'update', fieldUpdate)

      let dif
      if (entity) {
        dif = !_.isEqual(_.flatten([updatedEntity._source[field]]), _.flatten([entity._source[field]]))
      }

      const fieldSchema = entitySchema[field]
      const updatePromises = []

      if (dif && fieldSchema.copyTo) {
        const copyToRelations = _.flatten([fieldSchema.copyTo])
        updatePromises.push(
          DeepUpdater.updateCopyToSiblings(updatedEntity, copyToRelations, {set: {[field]: updatedEntity._source[field]}}, cache))
      }

      if (fieldSchema.unionIn) {
        updatePromises.push(
          DeepUpdater.recalculateUnionInSiblings(entity, updatedEntity, field, cache))
      }

      if (fieldSchema.isRelationship) {
        updatePromises.push(
          DeepUpdater.handleNewLinkages(updatedEntity, field, fieldUpdate, cache))
      }
      return Q.all(updatePromises)

    })
  }

  static handleNewLinkages(updatedEntity, field, fieldUpdate, cache) {
    return async.each(['addToSet', 'push', 'set'], (updateType) => {
      if (!fieldUpdate[updateType]) {
        return Q()
      }
      const e2Type = utils.fieldType(cache.es.config, updatedEntity._type, field)
      const values = [fieldUpdate[updateType][field]]
      //debug('handleNewLinkages values', values)
      const e2Entities =
        _(values)
        .flatten()
        .map((e2Id) => {
          return {
            _type: e2Type,
            _id: e2Id
          }
        })
        .value()
      //debug('handle possibly new linkages for type', updatedEntity._type, 'and _id', updatedEntity._id, 'with related entities of type', _.map(e2Entities, '_type'), field)
      return cache.es.deep.link({
          e1: updatedEntity,
          e1ToE2Relation: field,
          e2Entities: e2Entities
        },
        cache
      ).catch((err) => {
        debug('error in linking', err, updatedEntity, field, fieldUpdate, e2Entities)
      })
    })
  }

  static recalculateUnionInSiblings(entity, updatedEntity, field, cache) {
    const entitySchema = this.es.config.schema[updatedEntity._type]
    const fieldSchema = entitySchema[field]
    const unionInRelations = _.flatten([fieldSchema.unionIn])

    debug('recalculateUnionInSiblings: will calculate union on relations', unionInRelations, 'updatedEntity', updatedEntity._id, updatedEntity._type)

    return async.each(unionInRelations, (relation) => {
      const toUpdateEntityIds = updatedEntity._source[relation]
      //debug(updatedEntity._id, updatedEntity._type, relation, toUpdateEntityIds)
      if (!toUpdateEntityIds) {
        return Q()
      }
      //debug('recalculateUnionInSiblings', updatedEntity._type, updatedEntity._id, 'updated field ', field, 'relation', relation, 'relatedEIds', toUpdateEntityIds)
      if (!_.isArray(toUpdateEntityIds)) {
        toUpdateEntityIds = [toUpdateEntityIds]
      }
      return async.each(toUpdateEntityIds, (relatedEId) => {
        return utils.recalculateUnionInSibling(field, entity, updatedEntity, relation, relatedEId, cache)
      })
    })
  }

  static updateCopyToSiblings(updatedEntity, relations, fieldUpdate, cache) {
    return async.each(relations, (relation) => {
      const toUpdateEntityIds = updatedEntity._source[relation]
      if (!toUpdateEntityIds) {
        return Q()
      }
      if (!_.isArray(toUpdateEntityIds)) {
        toUpdateEntityIds = [toUpdateEntityIds]
      }
      return async.each(toUpdateEntityIds, (relatedEId) => {
        return utils.updateCopyToSibling(updatedEntity, relation, relatedEId, fieldUpdate, cache)
      })
    })
  }

  static pluckUpdatesForField(field, update) {
    return _.transform(update, (result, updateTypeInstructions, updateType) => {
      if (!_.isArray(updateTypeInstructions)) {
        updateTypeInstructions = [updateTypeInstructions]
      }
      updateTypeInstructions.forEach((instruction) => {
        if (instruction[field]) {
          result[updateType] = result[updateType] || {}
          result[updateType][field] = instruction[field]
        }
      })
    }, {})
  }
}

module.exports = DeepUpdater

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')
  const Cache = require('../cache')
  const cache = new Cache(es)
  es.deep.update({
    _id: 12,
    _type: 'a',
    update: {
      pull: {
        x: 567
      },
    }
  }, cache)
  .then((res) => {
    return cache.flush()
  })
  .then((res) => {
    return es.dsl.execute('get a 12')
    .then(debug)
  })
  .then((res) => {
    return es.dsl.execute('get b 1')
    .then(debug)
  })
  .catch(debug)
}
