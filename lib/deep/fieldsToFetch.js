var debug = require('debug')('fieldsToFetch')
var _ = require('lodash')

var configs = require('../../configs')

const forEntities = (entityTypes, context, lang) => {
  return _.chain(entityTypes)
    .reduce(function(soFar, entityType) {
      soFar.push(forEntity(entityType, context, lang))
      return soFar
    }, [])
    .flatten()
    .uniq()
    .value()
}

const forEntity = (entityType, context, lang) => {
  var contextConfig = _.isString(context) ? _.get(configs, context)[entityType] : context
  var schema = configs.schema[entityType]
  console.log("entityType-------",entityType)
  var toFetchFields = contextConfig.fields || []
  if (contextConfig.joins) {

    //Do union of joins and fields
    var toJoinFields = _.pluck(contextConfig.joins, 'fieldName')
    toFetchFields = toFetchFields.concat(toJoinFields)
  }
  if (contextConfig.primaryField) {
    toFetchFields.push(contextConfig.primaryField)
  }
  //debug(entityType, context, toFetchFields)

  //Add language prefix to fields
  return toFetchFields.map(function(field) {
    if (!_.includes(field, '.') && !schema[field]) {
      debug('Possible error: Didn not find schema for field', entityType, field)
    }
    return resolvePath(entityType, field, lang)
  })
}
/**
 * @param entityType
 * @param path sequence of relationships/properties without {language}, 'fields': arraay or . separated String
 * @param lang the language to be used
 * @return String . separated path with language filled in, 'fields' filled in.
 * Note: property names can be array also.
 */
const resolvePath = (entityType, path, lang) => {
  path = _.isString(path) ? path.split('.') : path
  var entityConfig = configs.schema[entityType]
  if (!entityConfig) {
    console.log('Error: fieldsToFetch: no entityConfig found for entity type', entityType, 'path',  path, lang, new Error().stack)
    throw new Error('Error: fieldsToFetch: no entityConfig found for entity type ' + entityType + ' path ' + path + ' lang' + lang)
  }
  return _.transform(path, (result, key) => {
    var fieldSchema = entityConfig[key]
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
      var relatedEntityType =  _.flatten([fieldSchema.type])[0]
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
logicalPath = (field, lang) => {
  field = _.isString(field) && field.split('.') || field
  return _(field).without(lang).without('fields').value().join('.')
}

/**
 * @param entityType for getting perticular entity fields
 * @param context e.g web.read | web.search
 * @param lang e.g english | tibetan
 */

const getFields = (entityType, context, lang) => {
  var fields = _.get(configs, context)[entityType].fields
  var primaryField = _.get(configs, context)[entityType].primaryField
  fields = _.without(fields, primaryField)

  primaryField = resolvePath(entityType, primaryField, lang)

  fields = _.map(fields, (field) => {
    return resolvePath(entityType, field, lang)
  })
  //console.log(entityType, context, fields, primaryField)
  return {fields: fields, primaryField: primaryField}
};

module.exports = {
  forEntities: forEntities,
  forEntity: forEntity,
  resolvePath: resolvePath,
  logicalPath: logicalPath,
  getFields: getFields
}

if (require.main === module) {
  console.log(resolvePath('language', 'name', 'english'))
  console.log(resolvePath('speaker', 'person.name', 'english'))
}
