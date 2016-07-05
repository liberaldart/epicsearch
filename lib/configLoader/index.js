'use strict'
const _ = require('lodash')
const toml = require('toml')
const async = require('async-q')
const fs = require('fs')
const debug = require('debug')('eps:configLoader')

const utils = require('../deep/utils')
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
    schema: {},
    //entityTypes: []
  }

  setJsonContentFromToml(config, 'elasticsearch', configFolderPath + '/elasticsearch.toml')
  setJsonContentFromToml(config, 'collect', configFolderPath + '/collect.toml')

  setFieldSchema(config, configFolderPath + '/schema/entities')
  setRelationships(config, configFolderPath + '/schema/relationships.txt')

  setUnions(config, configFolderPath + '/schema/union.toml')

  config.entityTypes = _.keys(config.schema)

  return config
}

const setFieldSchema = (config, schemaBasePath) => {
  const files = fs.readdirSync(schemaBasePath)
  files.forEach((fileName) => {
    const et = fileName.split('.')[0]
    setJsonContentFromToml(config.schema, et, [schemaBasePath, fileName].join('/'))

    //Now sanitize field config for String, Number, type classes
    _.each(config.schema[et], (fieldConfig, field) => {
      fieldConfig.type = sanitizedType(fieldConfig.type)
      const fieldBasicType = utils.fieldType(config, et, field)
      if (fieldBasicType === String && _.isUndefined(fieldConfig.multiLingual)) {
        fieldConfig.multiLingual = true
      }
    })
  })
}

const setJsonContentFromToml = (config, key, pathToTomlFile) => {
  //Load the schema for entity
  const fileContent = fs.readFileSync(pathToTomlFile)
  config[key] = toml.parse(fileContent)

  //debug('loaded toml file into config', key, config[key])
}

const sanitizedType = (inputType) => {
  const isListType = _.isArray(inputType)
  const basicType = (isListType && inputType[0]) || inputType
  switch (basicType) {
    case 'String': {return isListType && [String] || String}
    case 'Object': {return isListType && [Object] || Object}
    case 'Array': {return isListType && [Array] || Array}
    case 'Number': {return isListType && [Number] || Number}
    case 'Boolean': {return isListType && [Boolean] || Boolean}
    default: {return inputType}
  }
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

const setUnions = (config, configPath) => {

  const fileContent = fs.readFileSync(configPath).toString()
  let unions
  try {
    unions = toml.parse(fileContent)
  } catch (err) {
    debug('Error in parsing schema/unions file', err, err.stack)
    throw err
  }
  _.keys(unions).forEach((etA) => {
    //Iterate over each entity for which unions are specified
    const unionsForEtA = unions[etA]
    _.keys(unionsForEtA).forEach((aFieldToUnionFromB) => { //field or relationship
      //Iterate over every field of that entity, for which union is specified
      let aToBPaths = unionsForEtA[aFieldToUnionFromB]//aFieldToUnionFromB can get union from multiple sources
      aToBPaths = aToBPaths.replace(/\+/g, '').split(' ')
      _.set(config.schema, [etA, aFieldToUnionFromB, 'unionFrom'], aToBPaths)

      //Now iterate over every field union path and set the reverse path with unionIn
      //In respective end (B) nodes for every path
      aToBPaths.forEach((aToBPath) => {
        aToBPath = aToBPath.split('.')
        let reversePath = reverseRelationPath(config, etA, _.clone(aToBPath))//etB.{pathToA} TODO dont mutate aToBPath in reverseRelationPath function. Then no need to clone aToBPath

        const etB = _.first(reversePath)
        reversePath = _.drop(reversePath, 1) //Remove etB from start of the path
        reversePath.push(aFieldToUnionFromB) //Push aFieldToUnionFromB to end of the path

        const BSchema = config.schema[etB]
        const bFieldToUnionInA = _.last(aToBPath)
        //debug(etB + '\'s field to union in ' + etA + ' is ' + bFieldToUnionInA, etA + ' to ' + bFieldToUnionInA + '\'s path', aToBPath, 'reversePath', reversePath)
        BSchema[bFieldToUnionInA].unionIn = BSchema[bFieldToUnionInA].unionIn || []
        BSchema[bFieldToUnionInA].unionIn.push(reversePath) //Set this path in BSchema/bFieldToUnionInA
      })
    })
  })
}
/**
 * @param {Object} config
 * @param {String} etA - the starting entity from which pathTobField goes
 * @param {Array} pathTobField - the full path leading from etA to a field in etB
 * @return {Array} reversed path from B to A
 */
const reverseRelationPath = (config, etA, pathTobField)  => {
  const fieldOfB = _.last(pathTobField)
  const pathToB = _.dropRight(pathTobField, 1) //Create a slice with last one taken away

  let bToAPath = _(pathToB).map((forwardRelation) => {
    const forwardRelationSchema = config.schema[etA][forwardRelation]
    if (!forwardRelationSchema) { //TODO write a helpful error message to user to fix schema
      const err = 'No forward relationschema for A ' + etA + '. Relation is ' + forwardRelation + '. A\'s schema is ' + config.schema[etA] + ' pathToB is ' + pathToB
      throw new Error(err)
    }
    const reverseRelation = forwardRelationSchema.inName
    etA = forwardRelationSchema.to //Keep progressing etA with every relation forward
    return reverseRelation

  }).value()
  bToAPath = _.clone(bToAPath).reverse()
  const etB = etA //because we are at the end of the loop
  bToAPath.unshift(etB)//Push etB to start of bToAPath. It can be removed later by callee//TODO improve this
  return bToAPath
}

if (require.main === module) {
  const conf = module.exports(process.argv[2])
  debug(JSON.stringify(conf))
}

