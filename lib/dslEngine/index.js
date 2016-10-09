'use strict'
const async = require('async-q')
const R = require('ramda')
const  _ = require('lodash')
const Q = require('q')
const debug = require('debug')('DslEngine')
const objectUpdater = require('js-object-updater')
const ElsQuery = require('elasticsearch-query');
const esQuery = new ElsQuery();

const grammar = require('./grammar')
const val = require('./resolve')
const Cache = require('../cache')

const isTrue = require('./booleanExpression')
const executeAssignment = require('./assignment')

function DslEngine(es) {
  this.es = es
}
/**
 *  @param {String or Array} instructions - Single instruction or sequence of DSL instructions to execute
 *  @param {Object} ctx - Optional. The context for the instructions. Here the state of variables for instructions is stored.
 *  @return {Object} result from last instruction
 *
 */
DslEngine.prototype.execute = function(instructions, ctx) {
  instructions = (_.isArray(instructions) && instructions) || [instructions]
  instructions = organizeInstructions(instructions)
  ctx = ctx || new Cache()
  return executeDeep(instructions, ctx, this.es)
}

const executeDeep = (instructions, ctx, es) => {
  return async.eachSeries(instructions, (instruction) => {

    let mainInstruction = (_.isArray(instruction) && instruction[0]) || instruction
    if (_.isString(mainInstruction)) {
      return executeDslInstruction(mainInstruction, ctx, es)
    }
    // else is a function
    return mainInstruction.apply(null, [ctx])
  })
  .then((instructionsResponses) => {
    return _.last(instructionsResponses)
  })
}

const executeDslInstruction = (instruction, ctx, es) => {
  try {
    let parsedInstruction = (_.isArray(instruction) && instruction[0]) || instruction
    parsedInstruction = grammar.parse(parsedInstruction)
    if (_.isArray(instruction)) {
      parsedInstruction.childrenInstructions = _.last(instruction)
    }
    switch (parsedInstruction.command) {
      case 'get': {return executeGet(parsedInstruction, ctx, es)}
      case 'search': {return executeSearch(parsedInstruction, ctx, es)}
      case 'link': {return executeLink(parsedInstruction, ctx, es)}
      case 'unlink': {return executeUnLink(parsedInstruction, ctx, es)}
      case 'async': {return executeAsync(parsedInstruction, ctx, es)}
      case 'unset': {return executeUnset(parsedInstruction, ctx, es)}
      case 'addToSet': {return executeMemUpdate(parsedInstruction, ctx)}
      case 'add': {return executeMemUpdate(parsedInstruction, ctx)}
      case 'index': {return executeIndex(parsedInstruction, ctx, es)}
      case 'boolExpression': {return isTrue(parsedInstruction, ctx)}
      case 'assignment': {return executeAssignment(parsedInstruction, ctx, es)}
      default: {throw new Error('No handling found for command', parsedInstruction.command)}
    }
  }
  catch (e) {
    debug('Error in parsing', e.stack)
    throw e
  }
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
    _.set(ctx, type, getRes)
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
        query: query.query,
        size: (instruction.getFirst && 1) || 20
      })
      .then((searchRes) => {
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
          const res = searchRes.hits.hits[0]
          if (instruction.as) {
            ctx[instruction.as] = res
          }
          def.resolve(res)
        } else {
          if (instruction.as) {
            ctx[instruction.as] = searchRes
          }
          def.resolve(searchRes)
        }
      })
      .catch((err) => {
        debug('error is executeSearch', JSON.stringify(err))
        def.reject(err)
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
  return es.deep.index({
    index: type + 's',
    type: type,
    id: entity._id, //If not specified es will create one automatically
    body: doc
  })
}

const executeLink = (instruction, ctx, es) => {
  instruction = val(instruction, ctx)
  instruction.isOwn = true
  return es.deep.link(instruction)
}

const executeUnLink = (instruction, ctx, es) => {
  instruction = val(instruction, ctx)
  instruction.isOwn = true
  return es.deep.unlink(instruction)
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
  .then((res) => {
    entity._id = res._id
    return entity
  })
}

const executeUnset = (instruction, ctx, es) => {
  const docWithPath = instruction.docWithPath.split('.')
  const doc = val(_.first(docWithPath), ctx)
  const path = _.drop(docWithPath, 1)
   
  if (instruction.deepUpdate) {
    return es.deep.update({
      _id: doc._id,
      _type: doc._type,
      update: {
        unset: [{
          _path: path
        }]
      }
    })
  } //Else is only in memory update
  const updateInstruction = {
      doc: doc,
      update: {
        unset: path
      }
    }
  objectUpdater(updateInstruction)
  return Q(doc)
}

const executeMemUpdate = (instruction, ctx) => {
  const docWithPath = instruction.docWithPath.split('.')
  const doc = val(_.first(docWithPath), ctx)
  const path = _.takeRight(docWithPath, docWithPath.length - 1)
  const updateInstruction = {
      doc: doc,
      update: {}
    }
  let updateCommand = val(instruction.command, ctx)
  if (updateCommand === 'add') {
    updateCommand = 'push'
  }
  updateInstruction.update[updateCommand] = {
    _path: path,
    _value: val(instruction.value, ctx)
  }
  objectUpdater(updateInstruction)
  return doc
}


module.exports = DslEngine

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')
  /**es.dsl.execute([
    'async each *ids as ida', [
      'get test *ida as idaTest',
      'async each *ids as id', [
        'get test *id as x',
        'addToSet *idaTest._id in *x at _source.y',
        'index *x as type test'
      ]
    ]
  ], {ids: [1, 2]})**/
  const ctx = {x: {a: 2, b:[] }}
  es.dsl.execute(['unset *x.a', 'add 3 to *x.b', 'addToSet 3 to *x.b'], ctx)
  .then(() => {
    console.log(ctx)
  })
  .catch((e) => {
    console.log(e, e.stack)
  })
}
