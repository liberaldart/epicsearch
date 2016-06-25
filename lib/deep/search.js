'use strict'
const debug = require('debug')('search')
const _ = require('lodash')
const async = require('async-q')
const Q = require('q')
const fieldsToFetch = require('./fieldsToFetch')
const sanitize = require('./sanitizeEsResponse')

const Search = class Search {
  constructor(es) {
    this.es = es
  }

  execute(params, cache) {
    const query = this.query(params)
    const cached = cache && cache.get(query)
    debug('cache', cached)
    const sanitizeAndResolve = this.sanitizeAndResolve
    if (cached) {
      return Q(cached)
    }
    //else if not in cache
    return this.es.search(query)
    .then(function(res) {
      return sanitizeAndResolve(res.hits.hits, params)
      .then(function(sanitizedResponse) {
        res.hits.hits = sanitizedResponse
        return res
      })
    })
    .then((res) => {
      if (res.hits.total) {
        //Cache each entity in response
        res.hits.hits.forEach((hit) => {
          cache.setEntity(hit)
        })
      }
      //Also store full response
      cache && cache.set(query, res)
      return res
    })
  }

  sanitizeAndResolve(res, params) {
    return async.each(res, function(doc) {
      sanitize(doc, params.lang)
      return require('./resolveJoins')(doc, params.lang, params.context)
    })
  }

  query(params) {

    // debug('params', params)
    let index
    let type = params._type
    const fields = params.fields
    const toFetchFields = fields || fieldsToFetch.forEntities(this.es.config.entityTypes, params.context, params.lang)

    if (!type) {

      index = this.es.config.entityTypes.map(function(entityType) {
        return entityType + 's'
      })

      type = this.es.config.entityTypes

    } else if (_.isArray(type)) {

      index = _.map(type, function(entityType) {
        return entityType + 's'
      })

    } else if (!_.isArray(type)) {

      index = [type + 's']
      type = [type]

    }

    // Empty Query Search
    if (params.q === "") {

      return {
        index: index,
        body: {
          fields: toFetchFields,
          size: params.size || 10
        }
      }
    } else if (params.suggest) {

      const fieldsToQuery = _.chain(type)
        .reduce(function(soFar, entityType) {
          soFar.push(toQueryFields(entityType, params.lang))
          return soFar
        }, [])
        .flatten()
        .uniq()
        .value()

      // debug(fieldsToQuery)

      const multiMatch = {
        multi_match: {
          query: params.q,
          fields: fieldsToQuery
        }
      }

      return {
        index: index,
        type: type,
        body: {
          query: multiMatch,
          fields: toFetchFields,
          size: params.size || 10
        }
      }

    } else {

      const mustClauses = params.filters || []

      if (params.q) {
        mustClauses.push({
          query_string: {
            query: params.q
          }
        })
      }

      return {

        index: index,
        type: type,
        fields: toFetchFields,
        from: params.from || 0,
        size: params.size || 20,
        body: {
          query: {
            bool: {
              must: mustClauses
            }
          }
        }

      }
    }
  }

  toQueryFields(entityType, lang) {

    const entitySchema = this.es.config.schema[entityType]

    const fields = _.filter(_.keys(entitySchema), function(field) {

      return entitySchema[field]['autoSuggestion']
    })

    return _.map(fields, function(field) {

      if (entitySchema[field].multiLingual) {

        return lang + '.' + field + '.' + 'suggest'

      } else {

        return field + '.' + 'suggest'

      }

    })

  }
}

module.exports = Search

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')
  const Cache = require('../cache')
  const cache = new Cache(es)

  es.deep.search({
      q: 'franco',
      lang: 'english',
      context: 'web.search'
    }, cache)
    .then(function(res) {
      debug(JSON.stringify(res))
      return es.deep.search({
        q: 'franco',
        lang: 'english',
        context: 'web.search'
      }, cache)
      .then(function(res) {
        debug(JSON.stringify(res), cache.data)
      })
    })
    .catch(debug)
}
