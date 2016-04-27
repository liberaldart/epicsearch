var debug = require('debug')('sanitizeEsResponse')
var _ = require('lodash')

var configs = require('../../configs')

module.exports = function(esDoc, lang) {

  var schema = configs['schema'][esDoc._type]
  esDoc.fields = esDoc.fields || esDoc._source
  delete esDoc._source

  _.keys(schema).forEach(function(field) {

    var fieldSchema = schema[field]
    if (!_.isArray(fieldSchema.type)) {

      var fieldName = fieldSchema.multiLingual ? lang + '.' + field : field
      var fieldData = esDoc.fields[fieldName]
      if (fieldData) {
        esDoc.fields[fieldName] = fieldData[0]
      }
    }
  })

  unflatten(esDoc.fields)

  return esDoc
}

function unflatten(doc) {
  _.keys(doc).forEach(function(key) {
    var path = key.split('\.')
    var innerDoc = doc
    if (path.length > 1) {
      path.forEach(function(field, index) {
        if (!innerDoc[field]) {
          if (index < path.length - 1) {
            innerDoc[field] = {}
          } else {
            innerDoc[field] = doc[key]
            delete doc[key]
          }
        }
        innerDoc = innerDoc[field]
      })
    }
  })
}
