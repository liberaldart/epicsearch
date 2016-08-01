'use strict'
const debug = require('debug')('get')
const _ = require('lodash')
const deepMerge = require('deepmerge')

const fieldsToFetch = require('./fieldsToFetch')
const sanitize = require('./sanitizeEsResponse')

const Cache = require('../cache')

const Get = class Get {
  constructor(es) {
    this.es = es
  }

  /*
   * @param {String} _id
   * @param {String} _type 
   * @param {String} _index Optional. Default _type + 's' 
   * @param {String || [String]} langs - Optional. If not spcified, the full doc with all language data will be fetched 
   * @param {[String]} fields to fetch for this entity
   * @param {String || Object} joins The joins to do for this entity
   * */
  execute(params, cache) {
    cache = cache || new Cache(this.es)
    let cached = cache.get(params)
    if (cached) {//TODO pick only the fields from nested object, as required by the params. Because cache may have bigger object
      return Q(cached)
    }

    let toFetchFields = params.fields || fieldsToFetch.forEntity(this.es, params._type, params.joins, params.langs)
    if (toFetchFields.length === 0) {
      toFetchFields = undefined
    }
    const es = this.es
    return es.get.collect({
      index: params._index || params._type + 's',
      type: params._type,
      id: params._id,
      fields: toFetchFields
    })
    .then(function(esDoc) {

      sanitize(es, esDoc, params.langs)

      if (params.joins) {
        return require('./resolveJoins')(
          cache,
          params.langs,
          params.joins,
          esDoc
        )
      } else {
        //debug('no joins for', params._type, params, esDoc)
        return esDoc
      }
    })
    .then((joinedDoc) => {
      cached = cache.get(params._id + params._type) || cache.getEntity(params)
      if (!cached) {
        cache.setEntity(joinedDoc)
        cache.set(params, joinedDoc)
      } else {
        sanitize(es, cached)
        deepMerge(cached, joinedDoc)
      }
      //debug('cached data', params._type, JSON.stringify(cached), 'returned from es', JSON.stringify(joinedDoc), 'to fetch fields', toFetchFields)

      return joinedDoc 
    })
  }
}

module.exports = Get

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')
  const cache = new Cache(es)
  cache.es.get.collect({
    index: 'sessions',
    type: 'session',
    id: '1',
  })
  .then(function(res) {
    debug(JSON.stringify(res))
  })
  .catch(debug)
}
