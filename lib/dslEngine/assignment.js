'use strict'
const _ = require('lodash')
const Q = require('q')
const debug = require('debug')('epicsearch:dslEngine/assignment')

const isTrue = require('./booleanExpression')
const resolve = require('./resolve')

module.exports = (expression, ctx, es) => {
  let value = resolve(expression.assignment, ctx)
  if (expression.condition && !isTrue(expression.condition, ctx)) {
    value = resolve(expression.elseAssignment, ctx)
  }

  const assignee = resolve(expression.assignee, ctx)

  const updatePath = assignee.split('.')
  const doc = _.get(ctx, _.dropRight(updatePath, 1))//_source.title
  const fieldToUpdate = updatePath.length > 1 && _.last(updatePath)

  if (fieldToUpdate && expression.deepUpdate) { //Means it is an entity being updated and we wish to save it in ES
    //debug(entity, fieldToUpdate)
    return es.deep.update({
      _id: doc._id,
      _type: doc._type,
      update: {
        set: {
          [fieldToUpdate]: value
        }
      }
    })
  } 
  //Else is only in memory update
  _.set(ctx, updatePath, value)
  return Q(value)
}
