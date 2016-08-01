const debug = require('debug')('sanitizeEsResponse')
const _ = require('lodash')

module.exports = function(es, esDoc, langs) {
  const configs = es.config
  const schema = configs.schema[esDoc._type]

  esDoc.fields = esDoc.fields || esDoc._source
  delete esDoc._source

  langs = langs && _.flatten([langs]) || es.config.common.supportedLanguages
  
  _.keys(schema).forEach(function(field) {

    const fieldSchema = schema[field]

    //Elasticsearch makes array of non array fields, when fields is specified in query. So we change from array to single values for single cardinality fields 
    if (fieldSchema.cardinality === 'one') {

      if (fieldSchema.multiLingual) {

        langs.forEach((lang) => {

          const fieldName = lang + '.' + field
          const fieldData = _.get(esDoc, ['fields', fieldName])
          if (fieldData) {
            esDoc.fields[fieldName] = fieldData[0]
          }
        })
      } else {
        const fieldData = _.get(esDoc, ['fields', field])
        if (_.isArray(fieldData)) {
          esDoc.fields[field] = fieldData[0]
        }
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
