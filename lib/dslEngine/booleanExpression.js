'use strict'
const _ = require('lodash')
const isEqual = require('deep-equal')
const resolve = require('./resolve')

module.exports = (expression, ctx) => {
  switch (expression.subCommand) {
    case 'isEqual': {return executeEquals(expression, ctx)}
    case 'emptinessCheck': {return executeEmptinessCheck(expression, ctx)}
    default: {throw new Error('Supplied boolean expression invalid')}
  }
}

const executeEquals = (expression, ctx) => {
  const left = resolve(expression.left, ctx)
  const right = resolve(expression.right, ctx)
  const strictComparision = expression.strict? true: false
  return isEqual(left, right, {strict: strictComparision})
}

const executeEmptinessCheck = (expression, ctx) => {
  let val = resolve(expression.val, ctx)
  if (_.isString(val)) {
    val = val.trim()
  }

  const checkIsEmpty = !expression.negation
  const isEmpty = !_.isNumber(val) && _.isEmpty(val)
  return checkIsEmpty? isEmpty : !isEmpty
}
