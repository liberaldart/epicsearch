/**
 * Group the document set in given index/type by values of 'key' field, for keys on which atleast one duplicate has occured.
 * For each such value, get all the (duplicate) docs, ordered by sort (if provided)
 * Apart from the first doc in sorted list, delete all the others.
 *
 * The above process is repeated for batch_size number of values every time, till no duplicates are left
 *
 * @param doc (OR docs) The doc instances which are to be saved, and their duplicates removed
 * @param key The key by which the duplicates exist
 * @param source_key Optional. The key which will be available in doc._source, in the multi_field scenario. Default is key
 * @param index Optional. default is config.doc_index
 * @param type Optional. the type of documents to be deduped on, default is config.doc_type
 */

var debug = require('debug')('DupDelete')
var _ = require('lodash')

function DupDelete(es) {
  this.es = es
}

DupDelete.prototype.gobble = function(params) {
  return this.swallow(this.chew(params).instructions)
}

DupDelete.prototype.chew = function(params) {
  var docs = params.docs || params.doc
  if (!docs) {
    throw new Error('docs or doc not passed for indexing')
  } else if (!_.isArray(docs)) {
    docs = [docs]
  }
  params.docs = docs
  return {
    instructions: params,
    response_size: docs.length
  }
}

DupDelete.prototype.swallow = function(params) {
  var key = params.key
  var source_key = params.source_key || key
  var index = params.index
  var type = params.type
  var docs = params.docs
  var delExceptThese = del_except_these(docs,key,source_key)
  //A potentially big OR query with one clause for every unique value which has duplicates. That clause preserves the first (based on sort) document for each unique value
  debug(JSON.stringify(delExceptThese))
  return this.es.deleteByQuery({
    body: {
      query: delExceptThese
    },
    index: index,
    type: type
  })
  .then(function(del_res) {
    if (del_res && del_res.error) {
      throw new Error(del_res.error)
    }
    debug(JSON.stringify(del_res))
    return docs //The same input docs, which were deduped
  })
}

var del_except_these = function(docs, key, source_key) {
  var delInstructions =
    docs.map(function(doc) {
      if (!doc) {

        throw new Error('found a null or undfined doc in passed docs to delete_dups')
      }
      var keyEqVal = {}
      keyEqVal[key] = doc[source_key][0] || doc[source_key]//If fields is set, then ES returns array
      return {//Query for all docs with given key:val and NOT this docs _id
        bool: {
          must: {
            term: keyEqVal
          },
          must_not: {
            term: {
              _id: doc._id
            }
          }
        }
      }
    })
  var del_q = {
    bool: {
      should: delInstructions
    }
  }
  return del_q
}

module.exports = DupDelete

if(require.main === module) {

  var EpicSearch = require('../../../index')
  var config = require('../../../config')
  var es = new EpicSearch(config)
  
  es.delete_dups({
    index: 'test',
    type: 'test',
    docs: [{url: 12, _id: 1}],
    key: 'url'
  }).then(debug).catch(debug) 
}

