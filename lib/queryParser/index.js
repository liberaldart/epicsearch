'use strict'
const async = require('async-q')
const  _ = require('lodash')
const Q = require('q')
const debug = require('debug')('QueryParser')
const ElsQuery = require('elasticsearch-query');
const esQuery = new ElsQuery();

const grammar = require('./grammar')

function QueryParser(es) {
  this.es = es
}
/**
 *  @param {Array} instructions Sequence of DSL instructions to execute
 *  @param {params} the parameters for the instructions
 *  @return {Object} result from last instruction
 *
 */
QueryParser. prototype.parse = function(instructions, params) {
  const executionContext = _.cloneDeep(params)
  return async.eachSeries(instructions, (instruction) => {
    let parsedInstruction
    try {
      parsedInstruction = grammar.parse(instruction)
    } catch (e) {
      debug('Error in parsing', e.stack)
      throw e
    }
    switch (parsedInstruction.command) {
      case 'get': {return executeGet(parsedInstruction, executionContext, this.es)}
      case 'search': {return executeSearch(parsedInstruction, executionContext, this.es)}
      default: {throw new Error('No handling found for command', command)}
    }
  })
  .then((instructionsResponses) => {
    return _.last(instructionsResponses)
  })
}

const executeGet = (instruction, executionContext, es) => {
  const type = val(instruction.type, executionContext)
  const id = val(instruction.id, executionContext)
  return es.get.collect({
    index: type + 's',
    type: type,
    id: id
  })
  .then((getRes) => {
    instruction.as = instruction.as || instruction.type
    executionContext[instruction.as] = getRes
    return getRes
  })
}

const executeSearch = (instruction, executionContext, es) => {
  const type = val(instruction.type, executionContext)
  const where = val(instruction.where, executionContext)
  const def = Q.defer()
  esQuery.generate(type, where, null, {match: true}, (err, query) => {
    if (err) {
      def.reject(err)
    } else {
      es.search.collect({
        index: type + 's',
        type: type,
        query: query
      })
      .then((esRes) => {
        debug(esRes)
        def.resolve(esRes)
      })
      .catch((e) => {debug('eee', e)})
    }
  })
  return def.promise
}

const val = (o, executionContext) => {
  const resolve = val

  if (_.isArray(o)) {
    return o.transform((result, element) => {
      result.push(resolve(element))
    }, [])
  } else if (_.isObject(o)) {
    return _.transform(o, (result, val, key) => {
      key = resolve(key, executionContext)
      result[key] = resolve(val, executionContext)
    }, {})
  } else if (_.startsWith(o, '*')) { //It is a variable in executionContext
    return _.get(executionContext, _.trimLeft(o, '*'))
  } else {
    return o //Key is the value itself
  }
}

module.exports = QueryParser

if (require.main === module) {
  module.exports(['get test 1', 'get test 2'])
  .then(console.log)
  .catch(console.log)
}
