const debug = require('debug')('sanitizeEsResponse')
const _ = require('lodash')

const unflatten = require('../utils').unflatten

/**
 * Renames _source field to fields, in the docs deep tree
 * Purges some fields which are not requierd in the joined data 
 * Removes arrays for relations which are single in cardinality
 * Unflattens per field response structure of es into an Object
 *
 */

module.exports = function(es, esDoc, langs, force) {

  if (esDoc._source && !force) {
    return esDoc
  }

  langs = langs && _.flatten([langs]) || es.config.common.supportedLanguages

  unflatten(esDoc.fields)

  let newDoc = stripUnecessaryArrays(es, esDoc._type, esDoc.fields, 0, langs)

  esDoc.fields = newDoc.fields

  return esDoc
}

const stripUnecessaryArrays = (es, type, esDocFields, index, langs) => {

  const schema = es.config.schema
  let newDoc = {fields: {}}
  index = index || 0
  langs = langs || es.config.common.supportedLanguages

  const entitySchema = schema[type]
  _.keys(entitySchema).forEach(function(field) {

    const fieldSchema = entitySchema[field]

    //Elasticsearch makes array of non array fields, when fields is specified in query. So we change from array to single values for single cardinality fields
    if (!fieldSchema.isRelationship) {
      if (fieldSchema.multiLingual) {

        langs.forEach((lang) => {

          let fieldData = _.get(esDocFields, [lang, field])
          fieldData = _.isArray(fieldData) && fieldData || [fieldData]
          if (fieldData) {
            _.set(newDoc.fields, [lang, field], fieldData[index])
          }
        })
      } else {
        let fieldData = _.get(esDocFields, [field])
        fieldData = _.isArray(fieldData) && fieldData || [fieldData]
        newDoc.fields[field] = fieldData[index]
      }
    } else {

      if (esDocFields && esDocFields[field]) {
        if (fieldSchema.cardinality === 'many') {
          let ids = _.get(esDocFields, [field, '_id'])
          if (!_.isArray(ids)) {
            ids = [ids]
          }
          newJoinDocs = ids.map((id, i) => {

            let newJoinDoc = stripUnecessaryArrays(es, fieldSchema.to, esDocFields[field].fields, i, langs)
            newJoinDoc['_id'] = id

            return newJoinDoc
          })

          _.set(newDoc.fields, [field], newJoinDocs)

        }

        if (fieldSchema.cardinality === 'one') {

          let id = _.get(esDocFields, [field, '_id'])
          let newJoinDoc = stripUnecessaryArrays(es, fieldSchema.to, esDocFields[field].fields, 0, langs)

          newJoinDoc['_id'] = id[0]

          _.set(newDoc.fields, [field], newJoinDoc)
        }

      }
    }
  })

  return newDoc
}

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch(process.argv[2]) 
  const x = stripUnecessaryArrays(es, 'speaker', {primaryLanguages: {_id: '1', fields: { english: {name: 3}}}})
  const y = stripUnecessaryArrays(es, 'speaker', {primaryLanguages: {_id: ['1'], fields: { english: {name: [3]}, events: {_id: 2, fields: {title: 'tit'}}}}})
  const z = stripUnecessaryArrays(es, 'speaker', {primaryLanguages: {_id: ['1', '32'], fields: { english: {name: [3, 33]}, events: {_id: [2, 21], fields: {title: ['tit', 'fit']}}}}})
  const speaker = stripUnecessaryArrays(es, {"_index":"speakers","_type":"speaker","_id":"AVfllo5cYPCIqLGjYYhT","_version":1,"found":true,"_source":{"person":{"_id":"1"},"primaryLanguages":{"_id":"1"},"events":[{"_id":"1080","own":true}]},"isUpdated":true,"own":true}) 
  console.log(JSON.stringify(speaker))
}

