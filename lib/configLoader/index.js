'use strict'
const _ = require('lodash')
const toml = require('toml')
const async = require('async-q')
const fs = require('fs')
const debug = require('debug')('eps:configLoader')

const relationsFileGrammar = require('./relationshipFileGrammar')

/**
 * @param {String} configFolderPath To load and export config json from a config folder containing
 *      a. elasticsearch.toml
 *      b. collect.toml
 *      c. schema/relationships.txt
 *      d. schema/entities folder with {entity}.toml files
 **/
module.exports = function(configFolderPath) {
  const config = {
    schema: {}
  }

  setJsonContentFromToml(config, 'elasticsearch', configFolderPath + '/elasticsearch.toml')
  setJsonContentFromToml(config, 'collect', configFolderPath + '/collect.toml')

  setFieldSchema(config, configFolderPath + '/schema/entities')

  setRelationships(config, configFolderPath + '/schema/relationships.txt')

  return config
}

const setFieldSchema = (config, schemaBasePath) => {
  const files = fs.readdirSync(schemaBasePath)
  files.forEach((fileName) => {
    setJsonContentFromToml(config.schema, fileName.split('.')[0], [schemaBasePath, fileName].join('/'))
  })
}

const setJsonContentFromToml = (config, key, pathToTomlFile) => {
  const fileContent = fs.readFileSync(pathToTomlFile)
  config[key] = toml.parse(fileContent)
  //debug('loaded toml file into config', key, config[key])
}

const setRelationships = (config, relationshipFilePath) => {

  const fileContent = fs.readFileSync(relationshipFilePath).toString()

  const relationships = relationsFileGrammar.parse(fileContent)

  relationships.forEach((relationship) => { //A relationship name (forward and backward) may be shared between multiple entity pairs

    relationship.entityConnections.forEach((pair) => {

      //debug('relationship', relationship)

      //Set A to B relationship info in A's schema
      _.set(config.schema, [pair.a, relationship.aToBName], {
        isRelationship: true,
        type: pair.cardinalityB === 'many' ? [pair.b] : pair.b,
        to: pair.b,
        cardinality: pair.cardinalityB,
        inName: relationship.bToAName,
        inCardinality: pair.cardinalityA
      })

      //Set B to A relationship info in B's schema, if applicable
      if (!relationship.bToAName) {
        return
      }
      _.set(config.schema, [pair.b, relationship.bToAName], {
        isRelationship: true,
        type: pair.cardinalityA === 'many' ? [pair.a] : pair.a,
        to: pair.a,
        cardinality: pair.cardinalityA,
        inName: relationship.aToBName,
        inCardinality: pair.cardinalityB
      })

    })

  })
}

if (require.main === module) {
  const config = module.exports(process.argv[2])
  debug(JSON.stringify(config))
}
