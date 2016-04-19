var _ = require('lodash')
var mem_update = require('js-object-updater')

UPDATE = function(es) {

  this.es = es
}

UPDATE.prototype.gobble = function(params) {

  if (!params._id) {
    throw new Error("id missing")
  } else if (!params.type) {
    throw new Error("type missing")
  } else if (!params.update) {
    throw new Error('update missing')
  }

  var es =  this.es
  
  return es.get({
      index: params.index || params.type + "s",
      type: params.type,
      id: params._id
    })
    .then(function(res) {

      mem_update({
        doc: res._source,
        update: params.update,
        force: true
      })

      return es.index({
        index: params.index || params.type + "s",
        type: params.type,
        id: params._id,
        body: res._source
      })
    })
}

module.exports = UPDATE