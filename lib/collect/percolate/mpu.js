
/**
 * This module is based on registered queries in a query_index. These queries
 * and do updates on input documents themselves based on instructions written in the matched queries, in a json based DSL.
 * For any array of passed docs, a query index
 * find out which queries match each passed doc, and then update
 * corresponding doc with the update instructions provided in each matching
 * query for that doc
 */
/*
 * params = {
 *  "docs" : [{
 *      "text" : "value"
 *     }
 *  ],
 *  query_index:"queries"
 * },
 */
/*
 * @param query_index: The index where the queries to reverse-match are stored.
 *   if not specified, config.percolate.query_index is used
 * @param doc or docs: the document(s) to percolate and update
**/
var Q = require('q')
var utils = require('../utils')
var mem_update = require('js-object-updater')
var debug = require('debug')('MPU')
var _ = require('lodash')

function MPU(es) {
  this.es = es
}

MPU.prototype.gobble = function(params) {
  return this.swallow(this.chew(params))
}

MPU.prototype.chew = function(params) {

  if (!params.docs) {

    throw new Error('params.docs is undefined or null')
  }

  var query_index = params.query_index || this.es.config.percolate.query_index
  var docs = params.docs
  var  instructions = []

  docs.forEach(function(doc,i) {
    instructions.push({
      percolate: {
        index: query_index,
        type: doc._type//This is used to parse the document
      }
    })
    instructions.push({
      doc: doc
    })
  })
  return instructions
}


MPU.prototype.swallow = function(instructions) {

  var mpu = this

  return this.es.mpercolate({

    body: instructions

  }).
  then(function(res) {

    var docwise_query_ids = res.responses.map(function(per_doc_queries) {

      return per_doc_queries.matches.map(function(query) {return query._id})

    })
    var update_promises = docwise_query_ids.map(function(query_ids,i) {

      if (query_ids && query_ids.length) {

        return mpu.update_doc(instructions[2 * i + 1].doc, query_ids, instructions[2 * i].percolate.index)

      } else {

        return Q(instructions[2 * i + 1].doc)
      }
    })
    return Q.all(update_promises)
  })
}
/**
* Updates the given doc with update instructions in specified queries
* @param query_index = the index where queries are stored.
* By default config.percolate.query_index is used
* @param query_ids = ["competitor xyz's content","owned-content"]
* @param doc = the doc to be reverse matched and updated according to finally matched queries
**/
MPU.prototype.update_doc = function(doc, query_ids, query_index) {

  return this.get_queries(query_ids,query_index)
  .then(function(queries) {

    queries.forEach(function(query) {

      if (query) {

        if (!query.update) {

          console.log('mpu: found query without update param',JSON.stringify(query))

        } else {

          mem_update({doc: doc, update: query.update, query: query})

        }
      }
    })
    return doc
  })
}

MPU.prototype.get_queries = function(query_ids, query_index) {
  //We will do an mget to get all queries in one go
  //Map the query ids from ["1","2"] to [{_id:"1"},{_id:"2"}]
  //query_ids = query_ids.map(function(id) {return {_id:id}})
  return this.es.mget(
    {
      index: query_index,
      type: '.percolator',
      body: {ids: query_ids}
    }
  ).
  then(function(res) {
    return res.docs.map(function(doc) {
      return doc._source
    })
  })
}
module.exports = MPU

if (require.main === module) {
  var docs = [
    {
      text: 'marketing',
      _type: 'test'
    },
    {
      text: 'b2c',
      _type: 'test'
    }
  ]
  var docs2 = [
    {
      text: 'market',
      _type: 'test'
    },
    {
      text: 'b2b',
      _type: 'test'
    }
  ]
  var EpicSearch = require('../../../index')
  var config = require('../../../config')
  var es = new EpicSearch(config)
  es.mpu({
    query_index: 'queries',
    docs: docs
  })
  .then(function(res) {
    debug(1,JSON.stringify(res))
  })
  /**es.mpu({
    query_index: "queries",
    docs:docs2
  })
  .then(function(res) {
    debug(2,res)
  })**/
}
