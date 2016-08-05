'use strict'
const _ = require('lodash')
const debug = require('debug')('epicsearch:dslEngine/assignment')

const isTrue = require('./booleanExpression')
const resolve = require('./resolve')

module.exports = (expression, ctx, es) => {
  let value = resolve(expression.assignment, ctx)
  if (expression.condition && !isTrue(expression.condition, ctx)) {
    value = resolve(expression.elseAssignment, ctx)
  }

  const assignee = resolve(expression.assignee, ctx)

  if (expression.deepUpdate) { //Means it is an entity being updated and we wish to save it in ES
    const updatePath = expression.assignee.split('.')
    const entity = _.get(ctx, _.dropRight(updatePath, 1))//_source.title
    const fieldToUpdate = _.last(updatePath)
    //debug(entity, fieldToUpdate)
    return es.deep.update({
      _id: entity._id,
      _type: entity._type,
      update: {
        set: {
          [fieldToUpdate]: value
        }
      }
    })
  } //Else is only in memory update
  return Q(value)
}
