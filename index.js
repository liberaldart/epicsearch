'use strict'
const debug = require('debug')('EpicSearch/Index')
const elasticsearch = require('elasticsearch')
const _ = require('lodash')

const collectFunctions = {
    get: './lib/collect/get/index',
    mget: './lib/collect/get/mget',
    search: './lib/collect/search/index',
    msearch: './lib/collect/search/multi_search',
    bulk: './lib/collect/bulk',
    index: './lib/collect/index/index'
  }

const deepFunctions = {
    index: 'create',
    get: 'read',
    search: 'search',
    update: 'update'
  }

var EpicSearch = function(config) {
  if (typeof config === 'string') {//it is path to config
    config = require(config)
  }

  this.es = new elasticsearch.Client(_.clone(config.clientParams))

  this.es.config = config

  addQueryParser(this.es)
  addCollectFeature(this.es)
  addDeepFeature(this.es)
}
// create params for deep function
//       1. lang
//       2. context
//       3. type
//       4. fields
//       5. q
//       6. size
//       7. suggest
//       8. from
const addDeepFeature = (es) => {

  _.keys(deepFunctions)
  .forEach((fnName) => {
    var deepFunction = require('./lib/deep/' + deepFunctions[fnName])
    deepFunction = new deepFunction(es)
    es[fnName].deep = deepFunction.executes.bind(deepFunction)
  })
}

const addQueryParser = (es) => {
  const QueryParser = require('./lib/queryParser')
  es.queryParser = new QueryParser(es)
}

const addCollectFeature = (es) => {

  const Aggregator = require('./lib/collect/aggregator')
  const aggregator = new Aggregator(es.config)

  _.keys(collectFunctions)
  .forEach((fnName) => {

    const AggregatingFunction = require(collectFunctions[fnName])
    const fn = new AggregatingFunction(es)

    const aggregatedFn = function() {
      return aggregator.collect(fnName, fn, arguments)
    }

    es[fnName] = es[fnName] || aggregatedFn
    es[fnName].collect = aggregatedFn
  })
}

module.exports = function(config) {
  return new EpicSearch(config).es
}

