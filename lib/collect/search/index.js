/**
 * @param query in format of native es search Api
 *
 */

var _ = require('lodash')
var debug = require('debug')('Search')
function Search(es) {
  this.es = es
}

Search.prototype.gobble = function(query) {
  return this.swallow(this.chew(query))
}

Search.prototype.chew = function(query) {
  var def_index = query.index || this.es.config.default_index
  var def_type = query.type || this.es.config.default_type
  var search_type = query.searchType
 
  delete query.index
  delete query.type
  delete query.searchType

  return [
    {
      index: def_index,
      type: def_type,
      search_type: search_type
    },
    query
  ]
}

Search.prototype.swallow = function(m_search_instructions) {
  return this.es.msearch({
    body: m_search_instructions
  })
  .then((res) => {
    return res.responses
  })
}

Search.prototype.stripTheArrayResponse = true

module.exports = Search

if (require.main === module) {

  var EpicSearch = require('../../../index')
  var config = require('../../../config')
  var es = new EpicSearch(config)
  es.search.collect({ query:{term:{url:13}}})
  .then(function(res) {debug(JSON.stringify(res))})
  .catch(console.log)
  /**es.msearch({
    body: [{search_type:'count'},{query:{term:{url:1}}},{},{query:{term:{url:2}}}]
  })
  .then(function(res) {debug(JSON.stringify(res))})**/
}
