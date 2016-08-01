const debug = require('debug')('sanitizeEsResponse')
const _ = require('lodash')

/**
 * Renames _source field to fields, in the docs' deep tree
 * Purges some fields which are not requierd in the joined data 
 * Removes arrays for relations which are single in cardinality
 * Unflattens per field response structure of es into an Object
 *
 */
module.exports = function(es, esDoc, langs) {

  esDoc.fields = esDoc._source || esDoc.fields

  langs = langs && _.flatten([langs]) || es.config.common.supportedLanguages
  
  stripUnecessaryArrays(es.config.schema[esDoc._type], esDoc, langs)

  unflatten(esDoc.fields)

  return esDoc
}

const stripUnecessaryArrays = (entitySchema, esDoc, langs) => {

  _.keys(entitySchema).forEach(function(field) {

    const fieldSchema = entitySchema[field]

    //Elasticsearch makes array of non array fields, when fields is specified in query. So we change from array to single values for single cardinality fields 
    if (!_.isArray(fieldSchema.type)) {

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
}

function unflatten(doc) {
  _.keys(doc).forEach(function(key) {
    const path = key.split('\.')
    let innerDoc = doc
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
