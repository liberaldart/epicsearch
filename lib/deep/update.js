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
      var updatedEntity = _.cloneDeep(entity)
      updater({
        doc: updatedEntity._source,
        update: params.update,
        force: true
      })
      cache[updatedEntity._id + updatedEntity._type] = updatedEntity
      //debug('updateEntityAndSiblings', JSON.stringify(params))
      //debug('just set in cache', updatedEntity._source)
      return doAllUpdates(entity, updatedEntity, params, cache)
    })
  }

  static doAllUpdates(entity, updatedEntity, params, cache) {
    const entitySchema = cache.es.config.schema[updatedEntity._type]
    return async.each(_.keys(entitySchema), (field) => {
      var fieldUpdate = DeepUpdater.pluckUpdatesForField(field, params.update)
      if (entity && _.isEmpty(fieldUpdate)) {//If this field is being updated, and updatedEntity represents a new entity being created
        return Q()
      }
      //debug('percolate Updates', updatedEntity._type, updatedEntity._id, 'field', field, 'update', fieldUpdate)

      var dif
      if (entity) {
        dif = _.difference(_.flatten([updatedEntity._source[field]]), _.flatten([entity._source[field]])).length
      }
      var fieldSchema = entitySchema[field]
      var updatePromises = []

      if (dif && fieldSchema.copyTo) {
        var copyToRelations = _.flatten([fieldSchema.copyTo])
        updatePromises.push(
          utils.updateCopyToSiblings(updatedEntity, copyToRelations, fieldUpdate, cache))
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
      if (fieldUpdate[updateType]) {
        var e2Type = fieldType(this.es.config, updatedEntity._type, field)
        var values = [fieldUpdate[updateType][field]]
        //debug('handleNewLinkages values', values)
        var e2Entities =
          _(values)
          .flatten()
          .map((e2Id) => {
            return {
              _type: e2Type,
              _id: e2Id
            }
          })
          .value()
        debug('handle possibly new linkages for type', updatedEntity._type, 'and _id', updatedEntity._id, 'with related entities of type', _.map(e2Entities, '_type'), field)
        return cache.es.deep.link({
            e1: updatedEntity,
            e1ToE2Relation: field,
            e2Entities: e2Entities
          },
          cache
        ).catch((err) => {
          debug('error in linking', err, updatedEntity, field, fieldUpdate, e2Entities)
        })
      } else {
        return Q()
      }
    })
  }

  static recalculateUnionInSiblings(entity, updatedEntity, field, cache) {
    var entitySchema = this.es.config.schema[updatedEntity._type]
    var fieldSchema = entitySchema[field]
    var unionInRelations = _.flatten([fieldSchema.unionIn])

    debug('recalculateUnionInSiblings: will calculate union on relations', unionInRelations, 'updatedEntity', updatedEntity._id, updatedEntity._type)

    return async.each(unionInRelations, (relation) => {
      var toUpdateEntityIds = updatedEntity._source[relation]
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

  updateCopyToSiblings(updatedEntity, relations, fieldUpdate, cache) {
    return async.each(relations, (relation) => {
      var toUpdateEntityIds = updatedEntity._source[relation]
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

  static fieldType(config, entityType, field) {
    var fieldSchema = config.schema[entityType][field]
    return _.flatten([fieldSchema.type])[0]
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
  module.exports({"entities":[{"_id":"hd--Untitled--Event -3719/4741-2015-06-21-himachal-thekchen-choling-temple-ceremony","_type":"folder"}],"update":{"set":{"session":"AVOoOLCybOcXv7EUyIUZ"}},"context":"web.read","lang":"english"})
  .then(function(res) {
    debug('Updated', JSON.stringify(res))
  })
  .catch(debug)
}
