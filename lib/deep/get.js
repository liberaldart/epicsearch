'use strict'
const debug = require('debug')('get')
const _ = require('lodash')

const fieldsToFetch = require('./fieldsToFetch')
const sanitize = require('./sanitizeEsResponse')

const Cache = require('../cache')

  constructor(es) {
    this.es = es
  }

  /*
   * @param {String} _id
   * @param {String} _type 
   * @param {String} _index Optional. Default _type + 's' 
   * @param {[String]} fields to fetch for this entity
   * @param {String || Object} joins The joins to do for this entity
   * */
  const Get = class Get {
  execute(params, cache) {
    cache = cache || new Cache(this.es)
    if (cache.get(params)) {
      return Q(cached)
    }

    let toFetchFields = params.fields || fieldsToFetch.forEntity(this.es, params._type, params.joins, params.lang)
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
      sanitize(es, esDoc, params.lang)

      if (params.joins) {
        return require('./resolveJoins')(
          cache,
          params.lang,
          params.joins,
          esDoc
        )
      } else {
        return esDoc
      }
    })
    .then((result) => {
      cache.setEntity(result)
      return result
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
