'use strict'
const debug = require('debug')('create')
const async = require('async-q')
const Q = require('q')
const _ = require('lodash')

const Cache = require('../cache')

function Index(es) {
  this.es = es
}


const Create = class Create {
  constructor(es) {
    this.es = es
  }
  /**
   * Indexes the entity in es. Also stores it in cache, if cache is passed, replacing any older version of same entity from the cache
   * @param _id (or id) - optional
   * @param _type - type of entity
   * @param body - the entity body
   */

  execute(params, cache) {
    const es = this.es
    let flushCacheAtEnd = false
    if (!cache) {
      flushCacheAtEnd = true
      cache = new Cache(this.es)
    }

    return this.es.index.collect({
      index: (params._type || params.type) + 's',
      type: (params._type || params.type),
      id: (params._id || params.id),
      body: params.body
    })
    .then((res) => {
      const entity = {
        _id: res._id,
        _type: (params._type || params.type),
        _source: params.body
      }
      params._id = res._id

      cache.setEntity(entity)
      const DeepUpdater = require('./update')
      return DeepUpdater.doAllUpdates(null, entity, {update: {set: params.body}}, cache)
      .then(() => {
        if (flushCacheAtEnd) {
          return cache.flush()
        }
      })
    })
  }
}

module.exports = Create

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')
  const cache = new Cache(es)
  es.deep.index({id: 12, type: 'a', body: {x: 1}}, cache)
  .catch(debug)
  .then(debug)
}
