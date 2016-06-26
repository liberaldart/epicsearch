const debug = require('debug')('sanitizeEsResponse')
const _ = require('lodash')

module.exports = function(es, esDoc, lang) {
  const configs = es.config
  const schema = configs['schema'][esDoc._type]
  esDoc.fields = esDoc.fields || esDoc._source
  delete esDoc._source

  _.keys(schema).forEach(function(field) {

    const fieldSchema = schema[field]
    if (!_.isArray(fieldSchema.type)) {

      const fieldName = fieldSchema.multiLingual ? lang + '.' + field : field
      const fieldData = _.get(esDoc, ['fields',fieldName])
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
    const path = key.split('\.')
    const innerDoc = doc
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
