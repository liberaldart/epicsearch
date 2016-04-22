'use strict'
const async = require('async-q')
const  _ = require('lodash')
const debug = require('debug')('QueryParser')

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
    const parsedInstruction = grammar.parse(instruction)
  debug(parsedInstruction)
    switch (parsedInstruction.command) {
      case 'get': {return executeGet(parsedInstruction, executionContext, this.es)}
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

const val = (key, executionContext) => {
  if (_.startsWith(key, '*')) { //It is a variable in executionContext
    return _.get(executionContext, _.trimLeft(key, '*'))
  } else {
    return key //Key is the value itself
  }
}

module.exports = QueryParser

if (require.main === module) {
  module.exports(['get test 1', 'get test 2'])
  .then(console.log)
  .catch(console.log)
}
