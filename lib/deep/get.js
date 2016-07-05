'use strict'
const debug = require('debug')('get')
const _ = require('lodash')

const fieldsToFetch = require('./fieldsToFetch')
const sanitize = require('./sanitizeEsResponse')

const Cache = require('../cache')

const Get = class Get {
  constructor(es) {
    this.es = es
  }

  execute(params, cache) {
    cache = cache || new Cache(this.es)
    if (cache.get(params)) {
      return Q(cached)
    }

    let toFetchFields = params.fields || fieldsToFetch.forEntity(this.es, params._type, params.context, params.lang)
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

      if (params.context || params.joins) {
        return require('./resolveJoins')(
          cache,
          esDoc,
          params.lang,
          params.context,
          params.joins
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
