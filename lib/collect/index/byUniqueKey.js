
/**
 * Workaround for lack of unique id limitation of Elasticsearch. 
 * This helps you index (or override existing) docs based on 
 * "unique ids" stored in the key field. Uses get_first internally to 
 * get the first documents for given key, and then overwrites those 
 * documents with supplied docs
 *
 *@param index The index to insert in
 *@param type The type of document to be inserted
 *@param doc|docs the doc(s) to be saved. Can be array or single element. Response is array or single element accordingly
 *@param key The unique field by which to do matches and get 'first' document from ES to overwrite (if it exists)
 *@param sort Optional. In case duplicates exist, the one to be on top of sort result will be overwritten by the input doc. If this is not specified, the document that gets overwritten is arbitrarily chosen.
 */

var _ = require('lodash')
var debug = require('debug')('IndexByUnique')
var BulkIndex = require('./bulk')

function IndexByUnique(es) {
  this.es = es
  this.bulkIndex = new BulkIndex(es)
}

IndexByUnique.prototype.gobble = function(params) {
  return this.chew(params).then(this.swallow)
}

IndexByUnique.prototype.chew = function(params) {

  var stripTheArrayResponse = false
  var docs = params.docs || params.doc

  if (!docs) {

    throw new Error('docs or doc not passed for indexing')

  } else if (!_.isArray(docs)) {

    stripTheArrayResponse = true
    docs = [docs]

  }

  params.docs = docs
  params.val = _.pluck(docs, params.key)

  var that = this
  return this.es.get_first(params)
  .then(function(esDocs) {

    esDocs = _.pluck(esDocs,'doc')

    esDocs.forEach(function(esDoc,i) {
      if (esDoc) {
        docs[i]._id = esDoc._id
      }
    })

    return {
      instructions: that.bulkIndex.chew(params),
      response_size: docs.length,
      stripTheArrayResponse: stripTheArrayResponse
    }
  })
}

IndexByUnique.prototype.swallow = function(bulkInstructions) {

  return this.es.bulk({body: bulkInstructions})
  .then(function(res) {

    return res.items

  })
}

module.exports = IndexByUnique

if (require.main === module) {
  var EpicSearch = require('../../../index')
  var config = require('../../../config')
  var es = new EpicSearch(config)

  es.index_by_unique({
    key: 'url',
    index: 'test',
    type: 'test',
    sort: {sortField: 'desc'},
    match: 'match_phrase',
    doc: [{ url: '13', sortField: 23}, {url: '13', sortField: 24}]
  })
  .then(function(res) {
    debug('indexed by uniqueness', res)
  })
  .catch(function(err) {
    debug('err', err)
  })
}
