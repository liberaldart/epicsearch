{
  'use strict'
  const _ = require('lodash')
/**
[event]
  session{title}
  speaker.primaryLanguages{name}
**/
}

start
  = joinsDef

joinsDef = entityWiseJoinDef+

entityWiseJoinDef
  = newLine* entityType:word newLine joins:joinDeclaration+ (space / newLine)* {
    return {
      [entityType.substr(1, entityType.length -2)] : joins 
    }
  }

joinDeclaration
  = space+ path:(word '\.'?)+ '{' fields:((!',' word) ','? space?)+ '}' newLine {
    const res = {}
    path = _(path).flatten().compact().value()
    fields = _(fields)
      .flatten()
      .compact()
      .remove((item) => ![',', ' '].includes(item))
      .map((item) => item[1])
      .value()
    return {
      path: path[0].split('.'),
      fields: fields
    }
  }

word 
  = w:(letter / squareBracket / [\*\._-])+ {
    const word = w.join('').trim()
    if (/^\d+$/.test(word)) {
      return +word
    } else {
      return word
    }
  }

letter 
  = [a-zA-Z0-9]

squareBracket
  = '[' / ']'

space
  = ' ' / '\t'

newLine
  = '\n' / [\n\r]
