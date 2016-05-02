'use strict'
const isEqual = require('deep-equal')
const resolve = require('./resolve')

module.exports = (expression, executionContext) => {
  switch (expression.command) {
    case 'isEqual': {return executeEquals(expression, executionContext)}
    default: {throw new Error('Supplied boolean expression invalid')}
  }
}

const executeEquals = (expression, executionContext) => {
  const left = resolve(expression.left, executionContext)
  const right = resolve(expression.right, executionContext)
  const strictComparision = expression.strict? true: false
  return isEqual(left, right, {strict: strictComparision})
}
