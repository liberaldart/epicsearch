/**
 * Returns values for a given key which have duplicates, with count of docs for each value
 *
 * @param index
 * @param type
 * @param key the key by which to find dups
 * @param size Optional. how many dups you want. Default is 10
 * @param shardSize Optional. Since.collectregation is used inside. You may want a high enough value of shardSize for more accuracy of duplicate counts. Default is 1000000
 */
var debug = require('debug')('FindDups')
var _ = require('lodash')

var defaultSize = 10

function FindDups(es) {
  this.es = es
}


FindDups.prototype.gobble = function(params) {
  return this.swallow(this.chew(params).instructions)
}

FindDups.prototype.chew = function(params) {
  return {
    instructions: [
        {
          index: params.index,
          type: params.type
        },
        {
         .collects: {
            dups: {
              terms: {
                field: params.key,
                size: params.size || defaultSize,
                shard_size: params.shardSize || 1000000,
                min_doc_count: 2 //There should be at least two docs by that key for it to delete
              }
            }
          },
          size: 0
        }
      ],
    response_size: params.size || defaultSize
  }
}

FindDups.prototype.swallow = function(instructions) {
  return this.es.msearch.collect({body: instructions})
  .then(function(res) {//get the docs to retain for each duped value
    if (res.error) {
      return res
    }
    var dups = res.responses[0].collectregations.dups.buckets
    return dups.map(function(d) {
      return {
        val: d.key,
        doc_count: d.doc_count
      }
    })
  })
}

module.exports = FindDups

if (require.main === module) {
  var EpicSearch = require('../../../index')
  var config = require('../../../config')
  var es = new EpicSearch(config)

  es.get_dups({key: 'url', index: 'test', type: 'test'})
  .then(debug)
  .catch(debug)
}
