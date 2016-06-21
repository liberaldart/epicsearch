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
  const ctx = params
  return executeDeep(instructions, ctx, this.es)
}

const executeDeep = (instructions, ctx, es) => {
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
      case 'get': {return executeGet(parsedInstruction, ctx, es)}
      case 'search': {return executeSearch(parsedInstruction, ctx, es)}
      case 'link': {return executeLink(parsedInstruction, ctx, es)}
      case 'async': {return executeAsync(parsedInstruction, ctx, es)}
      case 'addToSet': {return executeMemUpdate(parsedInstruction, ctx)}
      case 'index': {return executeIndex(parsedInstruction, ctx, es)}
      case 'boolExpression': {return isTrue(parsedInstruction, ctx)}
      case 'assignment': {return executeAssignment(parsedInstruction, ctx)}
      default: {throw new Error('No handling found for command', parsedInstruction.command)}
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

const executeGet = (instruction, ctx, es) => {
  const type = val(instruction.type, ctx)
  const id = val(instruction.id, ctx)
  return es.get.collect({
    index: type + 's',
    type: type,
    id: id
  })
  .then((getRes) => {
    instruction.as = instruction.as || instruction.type
    ctx[instruction.as] = getRes
    return getRes
  })
}

const executeSearch = (instruction, ctx, es) => {
  const type = val(instruction.type, ctx)
  const where = val(instruction.where, ctx)
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
      .then((searchRes) => {
        debug(searchRes)
        if (instruction.createIfNeeded && !searchRes.hits.hits[0]) {
          return create(es, type, where)
          .then((esDoc) => {
            searchRes.hits.hits.push(esDoc)
            searchRes.hits.total = 1
            return searchRes
          })
        } else {
          return searchRes
        }
      })
      .then((searchRes) => {
        if (instruction.getFirst) {
          def.resolve(searchRes.hits.hits[0])
        } else {
          def.resolve(searchRes)
        }
      })
    }
  })
  return def.promise
}
/**
 * @param {Object} es the elasticsearch client
 * @param {String} type the type of entity to be created
 * @param {Object} entity which may be an object with _id/_type/_source or just the body for getting stored as _source
 */
const create = (es, type, entity) => {
  const doc =  _.omit(entity._source || entity, ['_id', '_type'])
  return es.index.collect({
    index: type + 's',
    type: type,
    id: entity._id, //If not specified es will create one automatically
    body: doc
  })
  .then((indexRes) => {
    return {
      _type: type,
      _index: type + 's',
      _id: indexRes._id,
      _source: doc
    }
  })
}

const executeLink = (instruction, ctx, es) => {
  instruction = val(instruction, ctx)
  return es.deep.link(instruction)
}

const executeAsync = (instruction, ctx, es) => {
  const args = _.map(instruction.args, (arg) => {
    return val(arg, ctx)
  })
  const asyncFunction = val(instruction.func, ctx)
  args.push(instruction)
  args.push(ctx)
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
   *@param {Object} ctx
   *@param {instruction} the instruction with childInstructions if applicable
   *
   */
  each: (items, instruction, ctx, es) => {
    return async.each(items, (item) => {
      ctx = R.assocPath(['immutable', instruction.as], item, ctx)
      return executeDeep(instruction.childrenInstructions, ctx, es)
    })
  }
}

const executeIndex = (instruction, ctx, es) => {
  const entity = val(instruction.entity, ctx)
  const type = instruction.type || entity._type
  if (!type) {
    throw new Error('type not specified for index operation', instruction)
  }
  return create(es, type, entity)
}

const executeMemUpdate = (instruction, ctx) => {
  const docWithPath = instruction.docWithPath.split('.')
  const doc = val(_.first(docWithPath), ctx)
  const path = _.takeRight(docWithPath, docWithPath.length - 1)
  const updateInstruction = {
      doc: doc,
      force: true,
      update: {}
    }
  const updateCommand = val(instruction.command, ctx)
  updateInstruction.update[updateCommand] = {
    _path: path,
    _value: val(instruction.value, ctx)
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
