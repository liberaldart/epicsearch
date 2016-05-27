var debug = require('debug')('read')
var _ = require('lodash')

var fieldsToFetch = require('./fieldsToFetch')
var sanitize = require('./sanitizeEsResponse')

function Get(es) {
  this.es = es
}

Get.prototype.execute = function(params) {
  var entitySchema = this.es.config.schema[params._type]
  var toFetchFields = params.fields || fieldsToFetch.forEntity(params._type, params.context, params.lang)
  // console.log("es---",this)

  //debug('toFetchFields', toFetchFields, new Error().stack)
  return this.es.get.collect({
      index: params._index || params._type + 's',
      type: params._type,
      id: params._id,
      fields: toFetchFields
    })
    .then(function(esDoc) {
      sanitize(esDoc, params.lang)

      if (params.context || params.joins) {
        return require('./resolveJoins')(
          esDoc,
          params.lang,
          params.context,
          params.joins
        )
      } else {
        return esDoc
      }
    })
}

module.exports = Get 

if (require.main === module) {
  module.exports({
      _type: "folder",
      _id: "hd--Untitled--Event -3719/4741-2015-06-21-himachal-thekchen-choling-temple-ceremony",
      lang: 'english',
      context: 'web.read'
    })
    .then(function(res) {
      debug(JSON.stringify(res))
    })
    .catch(debug)
}
