var _ = require('lodash')

/**
{
    type: "entitytype",
     _id: "id",
    fields: [
        'fieldname'
    ],
    joins: {
        "entityType": [
            "fieldname"
        ]
    }
}
**/

READ = function(es) {

  this.es = es
}

READ.prototype.gobble = function(params) {

  if (!params.type) {
    throw new Error("type missing")
  }

  var toFetchFields = params.fields
  if (toFetchFields && !toFetchFields.length) {
    toFetchFields = toFetchFields.concat(_.keys(params.joins))
  }

  return this.es.get({
      index: params.index || params.type + "s",
      type: params.type,
      id: params._id,
      fields: toFetchFields
    })
    .then(function(response) {
      if (params.joins) {
        return resolveJoins(response._source || response.fields, params.joins)
      } else {
        return response
      }
    })
}

function resolveJoins(doc, joins) {
  var mgetInstructions = []
  var joinFields = _.keys(joins)

  _.each(joinFields, function(joinField) {

    var toJoinValues = (_.isArray(doc[joinField]) && doc[joinField]) || [doc[joinField]]

    _.each(toJoinValues, function(id) {

      mgetInstructions.push({
        _index: joinField + 's',
        _type: joinField,
        _id: id,
        fields: joins[joinField].fields
      })

    })

    doc[joinField] = []
  })

  return this.es.mget({
    body: {
      docs: mgetInstructions
    }
  })
  .then(function(res) {

    _.each(res.docs, function(toJoinDoc) {
      doc[toJoinDoc._type].push(toJoinDoc._source || toJoinDoc.fields)
    })
    return res
  })
}

module.exports = READ