var debug = require('debug')('esMappingGenerator')
var _ = require('lodash')
var async = require('async-q')
var es = require('../backend/app/es')
var configs = require('./')


module.exports = function(entityType) {

  var entitySchema = configs.schema[entityType]
  var entityLanguages = configs.entityLanguages
  var mapping = initialMapping(entityType)
  var entityProperties = mapping.mappings[entityType].properties

  var fields = _.keys(entitySchema)

  _.each(fields, function(field) {

    var type = entitySchema[field].type

    if (entitySchema[field].multiLingual) {

      // assgining language mapping
      langMapping(mapping, entityType, entityLanguages)

      _.forEach(entityLanguages, function(language) {

        if (entitySchema[field].autoSuggestion) {

          mapping.mappings[entityType].properties[language].properties[field] = {
            type: 'string',
            fields: {
              [field]: {
                type: fieldType(type),
                analyzer: language
              },
              suggest: {
                type: fieldType(type),
                store: false,
                analyzer: 'autocomplete',
                search_analyzer: 'standard'
              }
            }
          }
        } else {

          mapping.mappings[entityType].properties[language].properties[field] = {
            type: fieldType(type),
            analyzer: language
          }

        }

      })

    } else if (entitySchema[field].autoSuggestion) {

      entityProperties[field] = {
        type: 'string',
        fields: {
          [field]: {
            type: fieldType(type),
            analyzer: 'english'
          },
          suggest: {
            type: fieldType(type),
            store: false,
            analyzer: 'autocomplete',
            search_analyzer: 'standard'
          }
        }
      }
    } else if (_.isPlainObject(entitySchema[field].type[0])) {

      entityProperties[field] = {
        dynamic: true,
        //type: 'Object', If specified, throws error in es 2.1. 
        properties: {}
      }

      var nestedProperties = entityProperties[field].properties

      _.forEach(_.keys(entitySchema[field].type[0]), function(nestedField) {

        nestedProperties[nestedField] = {
          type: fieldType(entitySchema[field].type[0][nestedField].type)
        }

      })

    } else if (_.isArray(entitySchema[field].type)) {

      mapping.mappings[entityType].properties[field] = {
        type: 'string'
      }
    } else {

      mapping.mappings[entityType].properties[field] = {
        type: fieldType(type)
      }
    }

  })

  return mapping
}


function fieldType(type) {

  if (type === String || _.isArray(type) && type[0] === String) {
    return 'string'
  } else if (type === Date) {
    return 'date'
  } else if (type === Boolean) {
    return 'boolean'
  } else if (type == Number) {
    return 'float'
  } else {
    return 'string'
  }
}


function langMapping(mapping, entityType, entityLanguages) {

  if (!mapping.mappings[entityType].properties[entityLanguages[0]]) {
    var langMapping = _.reduce(entityLanguages, function(result, language) {
      result[language] = {
        properties: {}
      }

      return result
    }, {})

    mapping.mappings[entityType].properties = langMapping

  }

  return mapping
}


function initialMapping(entityType) {

  var mapping = {
    mappings: {
      [entityType]: {
        dynamic: true,
        properties: {}
      }
    },
    settings: {
      analysis: {
        filter: {
          autocomplete_filter: {
            type: 'edge_ngram',
            min_gram: 1,
            max_gram: 20
          }
        },
        analyzer: {
          autocomplete: {
            type: 'custom',
            tokenizer: 'standard',
            filter: [
              'lowercase',
              'autocomplete_filter'
            ]
          }
        }
      }
    }
  }

  return mapping
}


if (require.main === module) {
  
  // run mapping generator on all entities mentions in configs.
  var mappingsOfEntities = _.reduce(configs.entityTypes, function(result, entityType) {
    result[entityType] = module.exports(entityType)
    return result
  }, {})
  var toRecreateIndices = process.argv[3] && process.argv[3].split(',') || configs.entityTypes 
  debug('recreating indices for types', toRecreateIndices)
  if (process.argv[2] === 'recreate') {
    return async.eachSeries(toRecreateIndices, (et) => {
      return es.indices.delete({index: et + 's'})
        .then(() => {
        debug('Deleted index', et + 's')
        })
      .catch((err) => {
        if (err.status == 404) {
          debug(et + 's', 'index does not exist. ignoring.')
        } else {
          debug('Unknown error. Please recheck', err)
        }
      })
    })
    .catch(debug)
    .then(() => {
      debug('Deleted all indices. Creating again with new mapping')
      return async.eachSeries(toRecreateIndices, (et) => {
        return es.indices.create({
          index: et + 's',
          body: mappingsOfEntities[et]
        })
        .then(() => {
          debug('Created index', et + 's')
        })
        .catch((err) => {
          debug('Error in creating index', et + 's', err, 'Mapping:', JSON.stringify(mappingsOfEntities[et]))
        })
      })
      .catch((err) => {
        debug('Error in creating indices', err)
      })
    })
    .then(() => {
      debug('recreated indices')
    } )
  } else {
    debug(JSON.stringify(mappingsOfEntities), 'Simply printed the mapping')
  }
}
