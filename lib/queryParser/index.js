'use strict'
const async = require('async-q')
const R = require('ramda')
const  _ = require('lodash')
const Q = require('q')
const debug = require('debug')('QueryParser')
const objectUpdater = require('js-object-updater')
const ElsQuery = require('elasticsearch-query');
const esQuery = new ElsQuery();

const grammar = require('./grammar')
const val = require('./resolve')

const isTrue = require('./booleanExpression')
const executeAssignment = require('./assignment')

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
  const executionContext = params
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
      case 'addToSet': {return executeUpdate(parsedInstruction, executionContext)}
      case 'index': {return executeIndex(parsedInstruction, executionContext)}
      case 'isEqual': {return isTrue(parsedInstruction, executionContext)}
      case 'assignment': {return executeAssignment(parsedInstruction, executionContext)}
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
      executionContext = R.assocPath(['immutable', instruction.as], item, executionContext)
      return executeDeep(instruction.childrenInstructions, executionContext, es)
    })
  }
}

const executeIndex = (instruction, executionContext) => {
  const entity = val(instruction.entity, executionContext)
  return es.index.collect({
    index: entity._index || instruction.type + 's',
    type: entity._type || instruction.type,
    id: entity._id,
    body: entity._source
  })
}

const executeUpdate = (instruction, executionContext) => {
  const updateInstruction = {
      doc: val(instruction.doc, executionContext),
      force: true,
      update: {}
    }
  const updateCommand = val(instruction.command, executionContext)
  updateInstruction.update[updateCommand] = {
    _path: val(instruction.path, executionContext).split('.'),
    _value: val(instruction.value, executionContext)
  }
  objectUpdater(updateInstruction)
}


module.exports = QueryParser

if (require.main === module) {
  var EpicSearch = require('../../index')
  var config = require('../../config')
  var es = new EpicSearch(config)
  es.queryParser.parse([
    'async each *ids as ida', [
      'get test *ida as idaTest',
      'async each *ids as id', [
        'get test *id as x',
        'addToSet *idaTest._id in *x at _source.y',
        'index *x as type test'
      ]
    ]
  ], {ids: [1, 2]})
  .then(console.log)
  .catch((e) => {
    console.log(e, e.stack)
  })
}
