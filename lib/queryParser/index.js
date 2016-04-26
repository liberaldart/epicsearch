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
QueryParser.prototype.parse = function(instructions, params) {
  instructions = organizeInstructions(instructions)
  const executionContext = _.cloneDeep(params)
  return executeDeep(instructions, executionContext, this.es)
}

const executeDeep = (instructions, executionContext, es) => {
  return async.eachSeries(instructions, (instruction) => {

    let parsedInstruction
    try {
      parsedInstruction = grammar.parse((_.isArray(instruction) && instruction[0]) || instruction)
      if (_.isArray(instruction)) {
        parsedInstruction.childrenInstructions = _.last(instruction)
      }
    } catch (e) {
      debug('Error in parsing', e.stack)
      throw e
    }
    switch (parsedInstruction.command) {
      case 'get': {return executeGet(parsedInstruction, executionContext, es)}
      case 'search': {return executeSearch(parsedInstruction, executionContext, es)}
      case 'async': {return executeAsync(parsedInstruction, executionContext, es)}
      default: {throw new Error('No handling found for command', command)}
    }
  })
  .then((instructionsResponses) => {
    return _.last(instructionsResponses)
  })
}

const organizeInstructions = (instructions) => {
  const result = []
  let prevWasString = false
  instructions.forEach((instruction, i) => {
    if (_.isArray(instruction)) {//Replace last instruction in result with [last, subInstructionArray]
      result[result.length - 1] = [result[result.length - 1], organizeInstructions(instruction)]
    } else {
      result.push(instruction)
    }
  })
  return result
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
        def.resolve(esRes)
      })
      .catch((e) => {debug('eee', e)})
    }
  })
  return def.promise
}

const executeAsync = (instruction, executionContext, es) => {
  const args = _.map(instruction.args, (arg) => {
    return val(arg, executionContext)
  })
  const asyncFunction = val(instruction.func, executionContext)
  args.push(instruction)
  args.push(executionContext)
  args.push(es)
  if (asyncFunctions[asyncFunction]) {
    return asyncFunctions[asyncFunction](...args)
  } else {
    throw new Error('Currently not handling async function', asyncFunction)
  }

}

const asyncFunctions = {
  /**
   *@param {Array} items
   *@param {Object} executionContext
   *@param {instruction} the instruction with childInstructions if applicable
   *
   */
  each: (items, instruction, executionContext, es) => {
    return async.each(items, (item) => {
      executionContext[instruction.as] = item
//      debug(JSON.stringify(instruction), executionContext)
      return executeDeep(instruction.childrenInstructions, _.cloneDeep(executionContext), es)
    })
  }
}

const val = (o, executionContext) => {
  const resolve = val

  if (_.isArray(o)) {
    return _.transform(o, (result, element) => {
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
