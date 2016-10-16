const debug = require('debug')('sanitizeEsResponse')
const _ = require('lodash')

/**
 * Renames _source field to fields, in the docs deep tree
 * Purges some fields which are not requierd in the joined data 
 * Removes arrays for relations which are single in cardinality
 * Unflattens per field response structure of es into an Object
 *
 */

module.exports = function(es, esDoc, langs) {

  if (esDoc._source) {
    return esDoc
  }

  langs = langs && _.flatten([langs]) || es.config.common.supportedLanguages

  unflatten(esDoc.fields)

  let newDoc = stripUnecessaryArrays(es.config.schema, esDoc._type, esDoc.fields, 0, langs)

  esDoc.fields = newDoc.fields

  return esDoc
}

const stripUnecessaryArrays = (schema, type, esDocFields, index, langs) => {

  let newDoc = {fields: {}}

  const entitySchema = schema[type]
  _.keys(entitySchema).forEach(function(field) {

    const fieldSchema = entitySchema[field]

    //Elasticsearch makes array of non array fields, when fields is specified in query. So we change from array to single values for single cardinality fields
    if (!fieldSchema.isRelationship) {
      if (fieldSchema.multiLingual) {

        langs.forEach((lang) => {

          const fieldData = _.get(esDocFields, [lang, field])
          if (fieldData) {
            _.set(newDoc.fields, [lang, field], fieldData[index])
          }
        })
      } else {
        const fieldData = _.get(esDocFields, [field])
        if (_.isArray(fieldData)) {
          newDoc.fields[field] = fieldData[index]
        }
      }
    } else {

      if (esDocFields && esDocFields[field]) {
        if (fieldSchema.cardinality === 'many') {
          let Ids = _.get(esDocFields, [field, '_id'])

          newJoinDocs = Ids.map((id, i) => {

            let newJoinDoc = stripUnecessaryArrays(schema, fieldSchema.to, esDocFields[field].fields, i, langs)
            newJoinDoc['_id'] = id

            return newJoinDoc
          })

          _.set(newDoc.fields, [field], newJoinDocs)

        }

        if (fieldSchema.cardinality === 'one') {

          let Id = _.get(esDocFields, [field, '_id'])
          let newJoinDoc = stripUnecessaryArrays(schema, fieldSchema.to, esDocFields[field].fields, 0, langs)

          newJoinDoc['_id'] = Id[0]

          _.set(newDoc.fields, [field], newJoinDoc)
        }

      }
    }
  })

  return newDoc
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
