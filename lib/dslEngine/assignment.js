'use strict'
const _ = require('lodash')

const isTrue = require('./booleanExpression')
const resolve = require('./resolve')

module.exports = (expression, ctx) => {
  let value = resolve(expression.assignment, ctx)
  if (expression.condition && !isTrue(expression.condition, ctx)) {
    value = resolve(expression.elseAssignment, ctx)
  }

  const assignee = resolve(expression.assignee, ctx)

  _.set(ctx, assignee, value)
  return value
}
