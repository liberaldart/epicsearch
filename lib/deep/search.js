var debug = require('debug')('search')
var _ = require('lodash')
var async = require('async-q')

var es = require('../es')
var configs = require('../../../configs/index')
var fieldsToFetch = require('./fieldsToFetch')
var sanitize = require('./sanitizeEsResponse')

module.exports = function(params) {

  return es.search(query(params))
    .then(function(res) {

      return response(res.hits.hits, params)
        .then(function(entityResponse) {

          res.hits.hits = entityResponse

          return res
        })

    })
}

function response(res, params) {
  return async.each(res, function(doc) {
    sanitize(doc, params.lang)
    return require('./resolveJoins')(doc, params.lang, params.context)
  })
}


function query(params) {

  // debug('params', params)
  var index
  var type = params._type
  var fields = params.fields
  var toFetchFields = fields || fieldsToFetch.forEntities(configs.entityTypes, params.context, params.lang)

  if (!type) {

    index = configs.entityTypes.map(function(entityType) {
      return entityType + 's'
    })

    type = configs.entityTypes

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

    var fieldsToQuery = _.chain(type)
      .reduce(function(soFar, entityType) {
        soFar.push(toQueryFields(entityType, params.lang))
        return soFar
      }, [])
      .flatten()
      .uniq()
      .value()

    // debug(fieldsToQuery)

    var multiMatch = {
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

    var mustClauses = params.filters || []

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

function toQueryFields(entityType, lang) {

  var entitySchema = configs.schema[entityType]

  var fields = _.filter(_.keys(entitySchema), function(field) {

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


if (require.main === module) {

  module.exports({
      q: 'franco',
      lang: 'english',
      context: 'web.search'
    })
    .then(function(res) {
      debug(JSON.stringify(res))
    })
    .catch(debug)
}
