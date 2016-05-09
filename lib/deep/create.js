var debug = require('debug')('create')
var async = require('async-q')
var Q = require('q')
var _ = require('lodash')
// var es = require('../es')
var configs = require('../../configs')

function Index(es) {
  this.es = es
}


/**
 * @param _id optional
 * @param _type
 * @param body
 */
Index.prototype.execute = function(params) {
  var cachedEntities = {}
  return this.es.index.collect({
    index: (params._type || params.type) + 's',
    type: (params._type || params.type),
    id: (params._id || params.id),
    body: params.body
  })
  .then((res) => {
    var entity = {
      _id: res._id,
      _type: (params._type || params.type),
      _source: params.body
    }
    params._id = res._id
    return require('./update').doAllUpdates(null, entity, {update: {set: params.body}}, cachedEntities)
  })
  .then((indexDocRes) => {
    return async.each(_.values(cachedEntities), (updatedEntity) => {
      return es.index.collect({
        index: updatedEntity._index,
        type: updatedEntity._type,
        id: updatedEntity._id,
        body: updatedEntity._source || {doc: updatedEntity.doc}
      })
    })
    .then(() => {
      cachedEntities[params._id + params._type] = cachedEntities[params._id + params._type] || {
        _index: params._type + 's',
        _type: params._type,
        _id: params._id,
        _source: params.body
      }
      cachedEntities.justCreated = cachedEntities[params._id + params._type]

      return cachedEntities
    })
  })
}


module.exports = Index 
if (require.main === module) {
  module.exports({"_type":"event","body":{"english":{"title":"dddddddd"},"startingDate":1546281000000,"endingDate":1003516200000,"venues":["1001"],"classifications":["44"],"languages":["9"],"speakers":[]}}/*{
    _type: 'file',
    _id: '1112',
    body: {
      folders: ['4'],
      processingStatus: 'edit'
    }
  }*/)
  .then(debug)
  .catch((err) => {debug(err, 'err')})
}

