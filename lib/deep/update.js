'use strict'
const debug = require('debug')('epicsearch:deep/update')
const updater = require('js-object-updater')
const async = require('async-q')
const Q = require('q')
const _ = require('lodash')

const outGraphUpdater = require('./graph/outboundUpdater')
const utils = require('./utils')
const Cache = require('../cache')
const resolveJoins = require('./resolveJoins')

/**
 *@param {[Object]} entities - Optional. For multiple entities to updated together. 
 Otherwise use _id and _type for single entity
 *@param {String} _id - Optional. If updating single entity only, then give its _id and type
 *@param {String} _type - Optional. If updating single entity only, then give its _id and type
 *@param {Boolean} isOwn - In case creating or removing a relation, specify whether 
 it is own or inferred relation from unionIn
 *@param {Object} update - the instructions to update as per js-object-updater npm
 */
const DeepUpdater = class DeepUpdater {
  constructor(es) {
    this.es = es
  }

  execute(params, cache) {
    let flushCacheAtEnd = false
    if (!cache) {
      flushCacheAtEnd = true
      cache = new Cache(this.es)
    }
    params.entities = params.entities || [{_id: params._id, _type: params._type}]
    return async.each(params.entities, (e) => {
      return this.updateEntityAndRelatedGraph(
        {
          _id: e._id,
          _type: e._type,
          update: params.update,
          isOwn: params.isOwn,
          dontHandleLinking: params.dontHandleLinking 
        },
        cache
      )
    })
    .then(() => {
      if (flushCacheAtEnd) {
        return cache.flush()
      } else {
        return Q()
      }
    })
    .then(() => {
      return utils.getEntity(cache, params._id, params._type)
    })
  }

  updateEntityAndRelatedGraph(params, cache) {
    params._type = params.type || params._type
    params._id = params._id || params.id
    const deepUpdater = this
    return utils.getEntity(cache, params._id, params._type, deepUpdater.es)
    .then((entity) => {
      const updatedEntity = entity
      entity = _.cloneDeep(entity)
      updater({
        doc: updatedEntity._source,
        update: params.update,
        force: true
      })
      //debug('applied in memory update. New entity', JSON.stringify(updatedEntity), 'update params', params.update)
      this.trimObject(updatedEntity._source)
      cache.setEntity(updatedEntity)//Replace entity in cache with updatedEntity
      if (!_.isEqual(entity, updatedEntity)) {
        cache.markDirtyEntity(updatedEntity)
      }
      return DeepUpdater.doAllUpdates(entity, updatedEntity, params, cache)
    })
  }

  static doAllUpdates(entity, updatedEntity, params, cache) {
    const entitySchema = cache.es.config.schema[updatedEntity._type]
    const joinIns = cache.es.config.invertedJoins.index[updatedEntity._type] //Load joins from the index config
    return async.each(_.keys(entitySchema), (field) => {
      const fieldUpdate = DeepUpdater.pluckUpdatesForField(field, params.update)
      
      if (entity && _.isEmpty(fieldUpdate)) {//If this field is being updated, and updatedEntity represents a new entity being created
        //debug('no update for field', field, ' in entity', updatedEntity._type)
        return Q()
      }
      const updatedField = field

      const fieldSchema = entitySchema[updatedField]
      
      let onUpdatePromise
      if (!fieldSchema.isRelationship) {

        const updatePromises = []

        if (joinIns && joinIns[field]) {
          updatePromises.push(
            outGraphUpdater.recalculateJoinsInDependentGraph(cache, updatedEntity, updatedField, fieldUpdate))
        } 

        if (fieldSchema.unionIn) {
          updatePromises.push(
            outGraphUpdater.recalculateUnionInDependentGraph(cache, entity, updatedEntity, updatedField))
        }

        onUpdatePromise = Q.all(updatePromises)

      } else if (!params.dontHandleLinking){
        onUpdatePromise = Q.all([ 
          DeepUpdater.handleRemovedLinks(cache, entity, updatedEntity, updatedField, fieldUpdate, params.isOwn),
          DeepUpdater.handleNewLinks(cache, updatedEntity, updatedField, fieldUpdate, params.isOwn)
        ])
        .then(() => {
          const relationUpdateInstruction = (fieldUpdate.set || fieldUpdate.addToSet || fieldUpdate.push)
          const toJoinEntities = relationUpdateInstruction && relationUpdateInstruction[updatedField]
          debug('before resolving joins', JSON.stringify(updatedEntity), ' same entity in cache', JSON.stringify(cache.get(updatedEntity._id + updatedEntity._type)), 'compulsory joins', updatedField, toJoinEntities && {[updatedField]: toJoinEntities})
          return resolveJoins(cache, null, 'index', updatedEntity, toJoinEntities && {[updatedField]: toJoinEntities})
        })
      }

      //TODO dont join all the relationships! Just the updated one
      return onUpdatePromise
      /**.then(() => {
        debug('after resolving joins', JSON.stringify(updatedEntity), ' same entity in cache', JSON.stringify(cache.get(updatedEntity._id + updatedEntity._type)))
        
      })**/
    })
  }
  
  static handleRemovedLinks(cache, entity, updatedEntity, field, fieldUpdate, isOwn) {
    let entitiesToPull

    if (fieldUpdate.unset) {
      entitiesToPull = entity._source[field]
    } else if (fieldUpdate.pull){

      entitiesToPull = fieldUpdate.pull._value || fieldUpdate.pull._values || fieldUpdate.pull[field] || _.get(fieldUpdate.pull[0], '_value') || _.get(fieldUpdate.pull[0], '_values')  //as per the options for pull in js-object-updater api

    } else { //There is no removed link
      return Q()
    }
    //debug('handle removed', updatedEntity, field, fieldUpdate)

    entitiesToPull = _.isArray(entitiesToPull) && entitiesToPull || [entitiesToPull]
    entitiesToPull = _.compact(entitiesToPull)

    if (_.size(entitiesToPull)) {
    
      const e2Type = cache.es.config.schema[updatedEntity._type][field].to
      return DeepUpdater.removeLinks(cache, updatedEntity, field, e2Type, entitiesToPull, isOwn)
      //.then(() => debug(updatedEntity))
    } else {
      return Q()
    }
  }

  static removeLinks(cache, updatedEntity, updatedField, e2Type, e2Entities, isOwn) {
    if (!_.size(e2Entities)) {
      return Q()
    }

    e2Entities =
      _.flatten(e2Entities)
      .map(
        (e2) => {
          return {
            _id: e2._id,
            _type: e2Type
          }
        }
      )
    return cache.es.deep.unlink({
        e1: updatedEntity,
        e1ToE2Relation: updatedField,
        e2Entities: e2Entities,
        isOwn: isOwn 
      },
      cache
    )
  }

  static handleNewLinks(cache, updatedEntity, field, fieldUpdate, isOwn) {
    /**if (fieldUpdate.addToSet || fieldUpdate.push || fieldUpdate.set) {
      debug('create new link', JSON.stringify(updatedEntity), field, fieldUpdate, 'isOwn', isOwn)
    }**/
    return async.each(['addToSet', 'push', 'set'], (updateType) => {
      if (!fieldUpdate[updateType]) {
        return
      }
      const e2Type = cache.es.config.schema[updatedEntity._type][field].to
      let e2Entities = [fieldUpdate[updateType][field]]
      //debug('handleNewLinkages e2Ids', e2Ids)
      e2Entities =
        _.flatten(e2Entities)
        .map(
          (e2) => {
            return {
              _id: e2._id,
              _type: e2Type
            }
          }
        )
      return cache.es.deep.link({
          e1: updatedEntity,
          e1ToE2Relation: field,
          e2Entities: e2Entities,
          isOwn: isOwn
        },
        cache
      )
    })
  }

  static pluckUpdatesForField(field, update) {
    return _.transform(update, (result, updateTypeInstructions, updateType) => {
      if (!_.isArray(updateTypeInstructions)) {
        updateTypeInstructions = [updateTypeInstructions]
      }
      updateTypeInstructions.forEach((instruction) => {
        //push/pull/set/addToSet
        if (instruction[field]) { 
          result[updateType] = result[updateType] || {}
          result[updateType][field] = instruction[field]
        
        //unset instruction of the form: 'fieldA' or ['fieldA', 'fieldB']
        } else if (instruction === field || _.includes(instruction, field)) {
          result[updateType] = result[updateType] || []
          if (!_.includes(result[updateType], field)) {
            result[updateType].push(field)
          }
        } //unset instruction of the form: {_path: ['fieldA']}
        else if (instruction._path && _.first(instruction._path) === field) {
          result[updateType] = result[updateType] || [] 
          result[updateType].push(instruction)
        }
      })
    }, {})
  }
  
  trimObject(o) {
    _.keys(o).forEach((k) => {
      if (_.isEmpty(o[k])) {
        delete o[k]
      }
    })
  }
}

module.exports = DeepUpdater

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')
  const Cache = require('../cache')
  const cache = new Cache(es)
  debug(DeepUpdater.pluckUpdatesForField('primaryLanguages', {"unset":[{"_path":["primaryLanguages"]}]}))
  es.deep.update({
    _id: 1,
    _type: 'video',
    update: {
      /**
      push: {
        x: [567],
        yString: ['ff']
      },
      addToSet: {
        x: [567],
        yString: ['ff']
      },
      set: {
        x: [567, 2],
        yString: ['ffg']
      },
      unset: [
        'x'
      ],**/
      pull: {
        yString: ['ffg']
      },
      push: {
        yString: ['ffi=g']
      },
    }
  }, cache)
  .then((res) => {
    debug(res)
    return cache.flush()
  })
  .catch(debug)
}
