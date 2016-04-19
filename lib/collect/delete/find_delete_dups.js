/**
 * Group the document set in given index/type by values of 'key' field, for keys on which atleast one duplicate has occured.
 * For each such value, get all the (duplicate) docs, ordered by sort (if provided)
 * Apart from the first doc in sorted list, delete all the others.
 *
 * The above process is repeated for batch_size number of values every time, till no duplicates are left
 *
 * @param key The key by which the duplicates have to be found .collectregated), searched and deleted
 * @param source_key The key which will be available in doc._source, in the multi_field scenario. Default is key
 * @param sort The sort (in es format) by which to sort the duplicates, and delete from second one onwards, for every unique key
 * @param batch_size the batch size of unique key values to dedup every time. Default value is 1000. Value to the tune of 10000 slows down a lot.
 * @param index default is config.doc_index
 * @param type the type of documents to be deduped on, default is epicdoc
 * @return the values for which duplicates were deleted
 */

var _ = require('lodash')
var Q = require('q')
var debug = require('debug')('FindDeleteDups')
var error = debug
error.log = console.log.bind(console)

function FindDeleteDups(es) {
  this.es = es
}

FindDeleteDups.prototype.gobble = function(params) {
  return this.swallow(this.chew(params).instructions)
}

FindDeleteDups.prototype.chew = function(params) {
  return {
    instructions: params,
    response_size: 1
  }
}

FindDeleteDups.prototype.swallow = function(params) {
  var that = this
  return delete_dups.apply(this,arguments).
  then(function(res) {
    if (res) {//some dups were deleted
      return that.gobble(params)//To delete till no dups are left
    }
    else {
      return 'deleted all dups'
    }
  })
}

module.exports = FindDeleteDups

var delete_dups = function(params) {

  var es = this.es

  params.size = params.batch_size || params.size || 500 //for getDups

  return es.get_dups(params)
  .then(function(dupVals) {

    if (dupVals.length) {

      dupVals = _.pluck(dupVals,'key')

      var searchHeader = {}
      var fields = [params.source_key || params.key]

      var bulkInstructions = dupVals.reduce(function(soFar, val) {

        soFar.push(searchHeader)

        var query =  {
          query: {

            term: {}

          },
          sort: params.sort,
          fields: fields
        }

        query.query.term[params.key] = val

        soFar.push(query)

        return soFar

      }, [])

      return es.msearch.collect({body: bulkInstructions})

    } else {

      return //no more dups

    }
  })
  .then(function(msearchRes) {

    if (!msearchRes) {

      return

    }
  debug(msearchRes)
    var idsToDelete = msearchRes.responses.reduce(function(soFar, valDocs) {

      soFar.push(_.pluck(valDocs.hits.hits.slice(1), '_id'))
      debug(JSON.stringify(valDocs.hits.hits))
      return soFar

    }, [])

    var esBulkDeleteInstructions = _.flatten(idsToDelete).map(function(id) {

      return { delete: {_id: id}}

    })
    return es.bulk({
      index: params.index,
      type: params.type,
      body: esBulkDeleteInstructions
    })

  })

}

if (require.main === module) {
  var EpicSearch = require('../../../index')

  var config = require('../../../config.js')
  var es = new EpicSearch(config)
  es.find_and_delete_dups({key: 'url', source_key: 'url', sort: {fetch_time: 'desc'}, index: 'test', type: 'test'}).
  then(function(res) {
    debug('final res', res)
    es.get_dups({key: 'a', source_key: 'a', sort: {fetch_time: 'desc'}, index: 'test', type: 'test'})
    .then(debug)
    .catch(error)
  })
  .catch(function(error) {
    error('errored out', error)
  })
}
