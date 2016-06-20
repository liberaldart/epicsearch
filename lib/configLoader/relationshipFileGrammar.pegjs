{
  'use strict'
  const _ = require('lodash')
}

start
  = relationsDef

relationsDef = relationDef+

relationDef
  = relationAB:word space* '<>'? space* relationBA:word? space* newLine connectedEntities:entityConnection+ {
    return {
      aToBName: relationAB,
      bToAName: relationBA,
      entityConnections: connectedEntities
    }
  }

entityConnection
  = space+ entityA:word space+ '<>' space+ entityB:word newLine? {
    const relationshipDef = {}
    if (_.startsWith(entityA, '[')) {
      entityA = _.trim(entityA, '[]')
      relationshipDef.cardinalityA = 'many'
    } else {
      relationshipDef.cardinalityA = 'one'
    }
    relationshipDef.a = entityA

    if (_.startsWith(entityB, '[')) {
      entityB = _.trim(entityB, '[]')
      relationshipDef.cardinalityB = 'many'
    } else {
      relationshipDef.cardinalityB = 'one'
    }
    relationshipDef.b = entityB
    return relationshipDef
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
  = [\n\r]
