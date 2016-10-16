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
  const parsedInstructions = organizeInstructions(instructions)
  ctx = ctx || new Cache()
  return executeDeep(parsedInstructions, ctx, this.es)
}

const executeDeep = (parsedInstructions, ctx, es) => {
  return async.eachSeries(parsedInstructions, (instruction) => {

    let mainInstruction = (_.isArray(instruction) && instruction[0]) || instruction
    if (_.isFunction(mainInstruction)) {
      return mainInstruction.apply(null, [ctx])
    }
    return executeDslInstruction(mainInstruction, ctx, es)
  })
  .then((instructionsResponses) => {
    return _.last(instructionsResponses)
  })
}

const executeDslInstruction = (parsedInstruction, ctx, es) => {
  
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
    case 'iterateOverIndex': {return iterateOverIndex(parsedInstruction, ctx, es)}
    case 'assignment': {return executeAssignment(parsedInstruction, ctx, es)}
    default: {throw new Error('No handling found for command', parsedInstruction.command)}
  }
}

const organizeInstructions = (instructions) => {
  const result = []
  let prevWasString = false
  instructions.forEach((instruction, i) => {
    if (_.isArray(instruction)) {//Replace last instruction in result with [last, subInstructionArray]
      result[result.length - 1].childInstructions = organizeInstructions(instruction)
    } else {
      if (_.isString(instruction)) {
        try {
          instruction = grammar.parse(instruction)
        }
        catch (e) {
          debug('Error in parsing', instruction, e.stack)
          throw e
        }
      }
      result.push(instruction)
    }
  })
  return result
}

const executeGet = (instruction, ctx, es) => {
  const type = val(instruction.type, ctx)
  const id = val(instruction.id, ctx)
  const joins = val(instruction.joins, ctx)
  return es.deep.get({
    _index: type + 's',
    _type: type,
    _id: id,
    joins: joins
  })
  .then((getRes) => {
    _.set(ctx, type, getRes)
    return getRes
  })
}

const executeSearch = (instruction, ctx, es) => {
  const type = val(instruction.type, ctx)
  const where = val(instruction.where, ctx)
  const joins = val(instruction.joins, ctx)
  const def = Q.defer()
  esQuery.generate(type, where, null, {match: true}, (err, query) => {
    if (err) {
      def.reject(err)
    } else {
      if (_.get(query.query, ['filtered','query','term','_type'])) {
        delete query.query.filtered.query
      } //esQuery is generating unecessary query to search by _type. Remove that
      const queryToEs = {
        _index: type + 's',
        _type: type,
        query: query.query,
        joins: joins,
        size: (instruction.getFirst && 1) || 20
      }
      es.deep.search(queryToEs)
      .then((searchRes) => {
        if (params.createIfNeeded && !searchRes.hits.hits[0]) {
          return create(es, type, where)
          .then((justCreatedDoc) => { //Now handle concurrency cases in creation of this entity
            return es.deep.search(queryToEs, cache, true) //Fetch this search result from cache
            .then((concurrentlyCreatedAndCached)=> { 
                if (concurrentlyCreatedAndCached.hits.hits[0]) { //The document has been created and cached in meanwhile by some other part of program, and also put into this search result (This same code, run elsewhere)
                  es.delete({index: type + 's', type: type, id: justCreatedDoc._id}).done()
                  return concurrentlyCreatedAndCached 
                } 
                // Else if no concurrent creation has happened, set this doc in searchResults
                //The following changes in searchRes will also be affected in the cached searchRes as searchRes is cached by query in cache object inside deep.search() function
                searchRes.hits.hits.push(justCreatedDoc)
                searchRes.hits.total = 1

                //return cached searchRes
                return searchRes
              })

            })      
                    } //else
        return searchRes
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
  const id = entity._id
  const doc =  _.omit(entity._source || entity.fields || entity, ['_id', '_type'])
  return es.deep.index({
    index: type + 's',
    type: type,
    id: id, //If not specified es will create one automatically
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
      return executeDeep(instruction.childInstructions, ctx, es)
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

/**
 * Instruction params
 * @param as
 * @param index 
 * @param childInstructions
 * @param batchSize
 * @param scrollDuration
 *
 */
const iterateOverIndex = (instruction, ctx, es) => {

  instruction.as = val(instruction.as || instruction.type, ctx)
  instruction.batchSize = val(instruction.batchSize, ctx)
  instruction.scrollDuration = val(instruction.scrollDuration, ctx)
  instruction.index = val(instruction.index, ctx)

  const query = {
    index: instruction.index,
    scroll: instruction.scrollDuration || '30s',
    size: instruction.batchSize || 100
  }
  
  return es.search(query)
  .then((res) => {
    if (_.isEmpty(res.hits.hits)) {
      //debug('empty response', res)
      return
    }
    return async.whilst(() => !_.isEmpty(res.hits.hits), () => {
      //debug('got hits', res.hits.hits.map((h) => h._id))
      return asyncFunctions.each(res.hits.hits, instruction, ctx, es)
      .then(() => {
        return es.scroll({
          scrollId: res._scroll_id,
          scroll: instruction.scrollDuration || '30s'
        })
        .then((scrollRes) => {
          res.hits.hits = scrollRes.hits.hits
          res._scroll_id = scrollRes._scroll_id
        })
      })
    })
  })
}

module.exports = DslEngine

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch(process.argv[2])

  const ctx = new Cache(es, {x: {a: 2, b:[] }})
  return es.dsl.execute([
    'iterate over events as event. Get 10 at a time.',
    [
      (ctx) => debug('ggg', ctx.immutable.event._id)
    ]
  ], ctx)
  .then(debug)
  .catch(debug)

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

  const search = ['search first event where {_id: "AVeuJeQ9jGz7t7QfUg_M"}. Join from search. Create if not exists']//, 'search event where {_id: 1} as event2']
  /**return iterateOverIndex({
    as: 'event',
    type: 'event',
    scrollDuration: '10s',
    childInstructions: [(ctx) => debug(ctx.as))],
    size: 5
  }, ctx, es)
  .catch((e) => debug(JSON.stringify(e)))
  .then(() => debug('done'))**/
}
