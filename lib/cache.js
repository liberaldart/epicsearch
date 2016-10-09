'use strict'
const debug = require('debug')('eps:cache')
const updater = require('js-object-updater')
const Q = require('q')
const _ = require('lodash')
const async = require('async-q')

const Cache = class Cache {
  constructor(es) {
    this.data = {}
    this.es = es
  }
  /**
  * @param {String} key - The key to get cached data for
  * @return the cached data for the given key
  **/
  get(key) {
    if (_.isObject(key)) {
      return this.data[JSON.stringify(key)]
    } else {
      return this.data[key]
    }
  }

  /**
   * @param {String} _type
   * @param {String} _id
   */
  getEntity(_type, _id) {
    return this.get(_id + _type)
  }

  /**
  * Stores entities in cache with key = _id + _type
  * @param {Object} entity - an entity with _id, _type in it
  **/
  setEntity(entity) {
    //debug('setting entity', entity, new Error().stack)
    this.data[entity._id + entity._type] = entity
  }

  /**
  * Sets JSON.stringify(key) = res
  * @param {} key - It is strigified as JSON
  * @param {} res - The object to be stored as value against the key
  **/
  set(key, res) {
    //debug('setting', key, res, new Error().stack)
    this.data[JSON.stringify(key)] = res
  }

  markDirtyEntity(entity) {
    this.markDirty(entity._id + entity._type)
  }

  markDirty(key) {
    let object = this.get(key)
    object.isUpdated = true
  }

  flush() {

    //Of all the data here, only the entities should be marked updated
    //Rest is read only. This is the expected state. If not happeningm, something is wrong elsewhere in the use of cache elsewhere 
    const updatedEntities =
      _.values(this.data)
      .filter((val) => val.isUpdated)

    //For every updated entity, flush it to ES
    //debug(updatedEntities)
    return async.each(updatedEntities, (updatedEntity) => {
      delete updatedEntity.isUpdated
      return this.es.index.collect({
        index: updatedEntity._index || updatedEntity._type + 's',
        type: updatedEntity._type,
        id: updatedEntity._id,
        body: updatedEntity.fields || updatedEntity._source
      })
    })
  }
}

module.exports = Cache
