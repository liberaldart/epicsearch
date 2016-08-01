'use strict'
const debug = require('debug')('epicsearch:deep/update')
const updater = require('js-object-updater')
const async = require('async-q')
const Q = require('q')
const _ = require('lodash')

const utils = require('./utils')
const Cache = require('../cache')
const resolveJoins = require('./resolveJoins')

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
    return async.each(params.entities, (e) => {
      return this.updateEntityAndSiblings(
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
      } else {
        return Q()
      }
    })
    .then(() => {
      return utils.getEntity(cache, params._id, params._type)
    })
  }

  updateEntityAndSiblings(params, cache) {
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
    return async.each(_.keys(entitySchema), (field) => {
      const fieldUpdate = DeepUpdater.pluckUpdatesForField(field, params.update)
      
      if (entity && _.isEmpty(fieldUpdate)) {//If this field is being updated, and updatedEntity represents a new entity being created
        //debug('no update for field', field, ' in entity', updatedEntity._type)
        return Q()
      }
      const fieldSchema = entitySchema[field]
      const updatePromises = []

      if (fieldSchema.unionIn) {
        updatePromises.push(DeepUpdater.recalculateUnionInGraph(entity, updatedEntity, field, cache))
      }

      if (fieldSchema.isRelationship) {
        updatePromises.push(DeepUpdater.handleRemovedLinks(cache, entity, updatedEntity, field, fieldUpdate))
        updatePromises.push(DeepUpdater.handleNewLinks(cache, updatedEntity, field, fieldUpdate))
      }
      return Q.all(updatePromises) 
      .then(() => {
        return resolveJoins(cache, null, 'index', updatedEntity)
      })
    })
  }

  static recalculateUnionInGraph(sourceEntityOld, sourceEntityNew, updatedField, cache) {
    const entitySchema = cache.es.config.schema[sourceEntityNew._type]
    const updatedFieldSchema = entitySchema[updatedField]
    const unionInPaths = _.flatten([updatedFieldSchema.unionIn])

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

  static handleRemovedLinks(cache, entity, updatedEntity, field, fieldUpdate) {
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
      return DeepUpdater.removeLinks(cache, updatedEntity, field, e2Type, entitiesToPull)
      //.then(() => debug(updatedEntity))
    } else {
      return Q()
    }
  }

  static removeLinks(cache, updatedEntity, updatedField, e2Type, e2Entities) {
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
          isOwn: true
      },
      cache
    )
  }

  static handleNewLinks(cache, updatedEntity, field, fieldUpdate) {
    //if (fieldUpdate.addToSet || fieldUpdate.push || fieldUpdate.set) {
    
      //debug('handle new', updatedEntity, field, fieldUpdate)
    //}
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
          isOwn: true
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
