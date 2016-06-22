'use strict'
const _ = require('lodash')

module.exports = (o, executionContext) => {

  if (_.isArray(o)) {
    return _.transform(o, (result, element) => {
      result.push(module.exports(element))
    }, [])
  } else if (_.isObject(o)) {
    return _.transform(o, (result, val, key) => {
      key = module.exports(key, executionContext)
      result[key] = module.exports(val, executionContext)
    }, {})
  } else if (_.startsWith(o, '*')) { //It is a variable in executionContext
    return _.get(executionContext.immutable, _.trimLeft(o, '*')) || _.get(executionContext, _.trimLeft(o, '*'))
  } else {
    return o //Key is the value itself
  }
}

if (require.main === module) {
  console.log(module.exports(
    {'*x': 4},
    {x: 2}
  ))
}
