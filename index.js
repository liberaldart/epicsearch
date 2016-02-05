var
  debug = require('debug')('EpicSearch/Index'),
  elasticsearch = require('elasticsearch'),
  _ = require('lodash'),
  fns = {
    get: './lib/get/index',
    mget: './lib/get/mget',
    search: './lib/search/index',
    msearch: './lib/search/multi_search',
    mpu: './lib/percolate/mpu',
    get_first: './lib/get/first',
    get_dups: './lib/get/dups',
    delete_dups: './lib/delete/delete_dups',
    find_and_delete_dups: './lib/delete/find_delete_dups',
    index_by_unique: './lib/index/byUniqueKey',
    bulk_index: './lib/index/bulk',
    bulk: './lib/bulk',
    index: './lib/index/index',
    crudRead: './lib/crud/read',
    crudUpdate: './lib/crud/update'
  }

var EpicSearch = function(config) {
  if (typeof config === 'string') {//it is path to config
    config = require(config)
  }

  this.es = new elasticsearch.Client(_.clone(config.clientParams))
  //this.es.native = {}
  if (config.cloneClientParams) {
    this.es.cloneClient = new elasticsearch.Client(_.clone(config.cloneClientParams))
  }

  this.es.config = config

  var Aggregator = require('./lib/aggregator')
  var aggregator = new Aggregator(config)
  var es = this.es

  _.keys(fns)
  .forEach(function(fnName) {

    var AggregatingFunction = require(fns[fnName])
    var fn = new AggregatingFunction(es)

    var aggregatedFn = function() {
      return aggregator.agg(fnName, fn, arguments)
    }

    es[fnName] = function() {//with whatever arguments
      var innerFunction = es[fnName] || aggregatedFn
      //do entity level role check
      var roleCheck = require('lib/' + fnName + '/roleCheck')

      return roleCheck.pre.apply(null, arguments)
      .then(function() { 
        return innerFunction.apply(null, arguments) 
      })
      .then(function() { 
        return roleCheck.post.apply(null, arguments)
      })
      
      //check document level r ole check
      //if check passes call innerFunction()
      //Filter out docs which are not allowed to be read by this role. doreturn response
    }

    es[fnName].agg = aggregatedFn
  })
}


module.exports = function(config) {
  return new EpicSearch(config).es
}

