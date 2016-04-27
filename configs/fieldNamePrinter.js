var schema = require('./index').schema
var _ = require('lodash')

var result = _(schema).transform((result, etSchema, et) => {
  result[et] = _.keys(etSchema)
}, {}).value()

console.log(result)
