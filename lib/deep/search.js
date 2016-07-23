'use strict'
const debug = require('debug')('search')
const _ = require('lodash')
const async = require('async-q')
const Q = require('q')
const fieldsToFetch = require('./fieldsToFetch')
const sanitize = require('./sanitizeEsResponse')
const Cache = require('../cache')

/***
 *
 *@param {String} q
 *@param {String} lang
 *@param {String} _type type of object to fetch
 *@param {String || Object} joins joins to do for given type
 *@param {[String]} fields fields to fetch for selected entity
 *@param {Integer} from
 *@param {Integer} size
 *@param {Boolean} suggest whether this is a suggest query or not
 *@param {[Object]} filters must clauses in elasticsearch format
 */
const Search = class Search {
  constructor(es) {
    this.es = es
  }

  execute(params, cache) {
    const query = this.query(params)
    cache = cache || new Cache(this.es)
    const cached = cache.get(query)
    if (cached) {
      return Q(cached)
    }

    const sanitizeAndResolve = this.sanitizeAndResolve
    const es = this.es
    //else if not in cache
    return es.search(query)
    .then(function(res) {
      return sanitizeAndResolve(cache, res.hits.hits, params)
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
      cache.set(query, res)
      return res
    })
  }

  sanitizeAndResolve(cache, hits, params) {
    return async.each(hits, function(doc) {
      sanitize(es, doc, params.lang)
      return require('./resolveJoins')(cache, params.lang, params.joins, doc)
    })
  }

  query(params) {

    // debug('params', params)
    let index
    let type = params._type
    const fields = params.fields
    let toFetchFields = fields || fieldsToFetch.forEntities(this.es, params.joins, params.lang)
    if (toFetchFields.length === 0) {
      toFetchFields = undefined
    }

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
debug(toFetchFields, index, type)
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
      lang: 'english',
      fields: ['x'],
      //context: 'web.search'
    }, cache)
    .then(function(res) {
      return es.deep.search({
        lang: 'english',
        fields: ['x']
      }, cache)
      .then(function(res) {
        debug(JSON.stringify(res))
      })
    })
    .catch(debug)
}
