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
    index: './lib/collect/index/index',
  }

var EpicSearch = function(config) {
  if (typeof config === 'string') {//it is path to config
    config = require(config)
  }

  this.es = new elasticsearch.Client(_.clone(config.clientParams))

  this.es.config = config

  addCollectFeature(this.es)
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

