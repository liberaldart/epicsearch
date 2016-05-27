'use strict'
const _ = require('lodash')
const debug = require('debug')('configs/index')
const config = require('../config')

const configsToLoad = config.configsToLoad
const entities = config.entities
const languages = config.languages

const entityConfigs = {}

configsToLoad.forEach(function(path) {

  path = path.split('.')
  let configAtPath = entityConfigs

  path.forEach(function(key) { //Create the path for nested configuration
    configAtPath = configAtPath[key] = configAtPath[key] || {}
  })

  entities.forEach(function(entityType) {
    const configName =  entityType
    try {
      let configPath = './' + path.join('/') + '/' + configName
      _.get(entityConfigs, path)[entityType] = require(configPath)
    } catch (err) {
      debug('could not find', configName, ' at ', path + '. Or perhaps there was another runtime error in loading the module? Ignoring this config')
    }
  })
})

// console.log("entityConfigs-------",entityConfigs)
/**
 * @param entityType  the type of entity to start digging into
 * @param path        the dot separated path or array of fields, to follow
 * @return            config of the leaf field of the path
 */
const getFieldSchema = (entityType, path) => {
  path = _.isString(path) && path.split('.') || path
  let entitySchema = entityConfigs.schema[entityType]
  let fieldSchema
  path.forEach((field) => {
    fieldSchema = entitySchema[field]
    if (fieldSchema.isRelationship) {
      entityType = _.flatten([fieldSchema.type])[0]
      entitySchema = entityConfigs.schema[entityType]
    }
    //console.log(entityType, field, path, fieldSchema, entitySchema)
  })
  return fieldSchema
}

module.exports = entityConfigs
module.exports.entityTypes = entities
module.exports.supportedLanguages = languages
module.exports.getFieldSchema = getFieldSchema
