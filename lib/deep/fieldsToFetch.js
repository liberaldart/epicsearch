'use strict'
const debug = require('debug')('fieldsToFetch')
const _ = require('lodash')

/**
 * Returns union of all the fields to fetch for every
 * entity type in given entity types
 *
 */
const forEntities = (es, joinConfigPathOrConfig, lang) => {
  return _.chain(es.config.entityTypes)
    .reduce(function(soFar, entityType) {
      soFar.push(forEntity(es, entityType, joinConfigPathOrConfig, lang))
      return soFar
    }, [])
    .flatten()
    .uniq()
    .value()
}

const forEntity = (es, entityType, joinConfigPathOrConfig, lang) => {
  
  const configs = es.config
  const schema = configs.schema[entityType]

  const joinConfig = _.isString(joinConfigPathOrConfig) ? _.get(configs, [joinConfigPathOrConfig, entityType]) : joinConfigPathOrConfig

  const toFetchFields = _.keys(joinConfig)
  
  //Add language prefix to fields
  return toFetchFields.map(function(field) {
    if (!_.includes(field, '.') && !schema[field]) {
      debug('Possible error: Did not find schema for field', entityType, field)
    }
    return resolvePath(entityType, field, lang)
  })
}
/**
 * Makes language and relationship join aware path, from logical path. if logical path is sessions.speakers.person.name, the full path, which is language and join aware, is sessions.fields.speakers.fields.person.fields.{lang}.name
 *
 * @param entityType
 * @param path sequence of relationships/properties without {language}, 'fields': arraay or . separated String
 * @param lang the language to be used
 * @return String . separated path with language filled in, 'fields' filled in.
 */
const resolvePath = (entityType, path, lang) => {
  path = _.isString(path) ? path.split('.') : path
  const entityConfig = configs.schema[entityType]
  if (!entityConfig) {
    console.log('Error: fieldsToFetch: no entityConfig found for entity type', entityType, 'path',  path, lang, new Error().stack)
    throw new Error('Error: fieldsToFetch: no entityConfig found for entity type ' + entityType + ' path ' + path + ' lang' + lang)
  }
  return _.transform(path, (result, key) => {
    const fieldSchema = entityConfig[key]
    if (!fieldSchema) {
      console.log('Error: no fieldSchema found for', key, ' in entityType', entityType, new Error().stack)
      throw new Error('Error: no fieldSchema found for ' + key + ' in entityType ' + entityType)
    }
    if (fieldSchema.isRelationship) {
      result.push(key)
      if (path.length > 1) {//If we have to go further inside the entity, then suffix with 'fields', else leave relationship name as it is
        result.push('fields') //for reading elasticsearch response which has fields
      }
      //Assuming there is only one entity in the relationship. Or, even if there
      //are multiple entities, the remaining path from this key is common to all
      const relatedEntityType =  _.flatten([fieldSchema.type])[0]
      entityConfig = configs.schema[relatedEntityType]
    } else {
      if (fieldSchema.multiLingual) {
        result.push(lang)
        result.push(key)
      } else {
        result.push(key)
      }
    }
  }, []).join('.')

}

/**
 * @param {String || Array} field Can be . separated String or array of fields
 * @return {String} The path without language or 'field' or '_source' keywords
 *
 */
const logicalPath = (field, lang) => {
  field = _.isString(field) && field.split('.') || field
  return _(field).without(lang).without('fields').value().join('.')
}

module.exports = {
  forEntities: forEntities,
  forEntity: forEntity,
  resolvePath: resolvePath,
  logicalPath: logicalPath,
}

if (require.main === module) {
  console.log(resolvePath('language', 'name', 'english'))
  console.log(resolvePath('speaker', 'person.name', 'english'))
}
