/**
 * Same format as native ES get Api
 * @param index
 * @param type
 * @param id
 */

var _ = require('lodash')
var debug = require('debug')('Get')
var error = debug
error.log = console.log.bind(console)

function Get(es) {
  this.es = es
}

Get.prototype.gobble = function(query) {
  debug('about to gobble', query)
  return this.swallow(this.chew(query).instructions)
}

Get.prototype.chew = function(query) {

  var instruction = _.clone(query)
  instruction._index = query.index
  delete instruction.index
  instruction._type = query.type
  delete instruction.type
  instruction._id = query.id
  delete instruction.id
  return {
    instructions: [instruction],
    response_size: 1
  }
}

Get.prototype.swallow = function(mget_instructions) {
  if (!_.isArray(mget_instructions)) {
    mget_instructions = [mget_instructions]
  }
  //debug(mget_instructions)
  return this.es.mget.agg({
      body: {
        docs: mget_instructions
      }
    })
    .then(function(res) {
      return res.docs
    })
}

Get.prototype.stripTheArrayResponse = true

module.exports = Get

if (require.main === module) {

  var EpicGet = require('../../index')
  var config = require('../../config')
  var es = new EpicGet(config)
  es.get.agg({
      id: 1,
      index: 'test',
      type: 'test'
    })
    .then(function(res) {
      debug('1', JSON.stringify(res))
    })
}
