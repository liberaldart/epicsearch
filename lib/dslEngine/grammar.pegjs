{
  'use strict'
  const _ = require('lodash')
}

start
  = statement

statement
  = get / search / async / index / link / booleanExpression / assignment / unset 

link
  = 'link' e1:word space+ 'with' space+ e2:word space+ 'as' space+ e1ToE2Relation:word {
      return {
        command: 'link',
        e1: e1,
        e2: e2,
        e1ToE2Relation: e1ToE2Relation
      }
    }

unset
  = 'unset' entityWithProperty:word  deepUpdate:('\.'? space? 'Do deep update' '.'?)? {
    return {
      command: 'unset',
      docWithPath: entityWithProperty,
      deepUpdate: deepUpdate
    }
  }

add
  = update: memUpdate {
    return update
  }

addToSet
  = update: memUpdate {
    return update
  }

memUpdate
  = command:word val:word space+ ('at' / 'in' / 'to') space+ docWithPath:word deepUpdate:('.' space? 'Do deep update' '.'?)? {
      return {
        command: command,
        docWithPath: docWithPath,
        deepUpdate: deepUpdate,
        value: val
      }
    }

assignment
  = assignee:word (' is' / ' are') space* value:jsonOrWord conditionalAssignment:conditionalAssignment? {
    const res = {
      command: 'assignment',
      assignee: assignee,
      assignment: value
    }
    if (conditionalAssignment) {
      _.extend(res, conditionalAssignment)
    }
    return res
  }

conditionalAssignment
  = ' if' condition:booleanExpression '.'? space+ 'Else ' ('is' / 'are')? value:word {
    return {
      condition: condition,
      elseAssignment: value,
    }
  }

booleanExpression
  = expr:(isEqual / emptinessCheck) {
    return expr
  }

isEqual
  = left:word ' is' right:(!'\?' word) strict:' strict'? space* '?' {
    return {
      command: 'boolExpression',
      subCommand: 'isEqual',
      left: left,
      right: right[1],
      strict: strict
    }
  }

emptinessCheck
  = val:jsonOrWord ' is' negation:' not'? ' empty' {
    return {
      command: 'boolExpression',
      subCommand: 'emptinessCheck',
      val: val,
      negation: negation
    }
  }

index
  = 'index' entity:word type:(' as type' word)? {
      return {
        command: 'index',
        entity: entity,
        type: type && type[1]
      }
    }

async
  = 'async' func:word args:(!" as" word)+ space+ 'as' as:word {
      return {
        command: 'async',
        func: func,
        args: _(args).flatten().compact().value(),
        as: as
      }
    }

get
  = 'get' type:word id:word as:as? {
      return _.omit({
        command: 'get',
        type: type,
        id: id,
        as: as
      }, _.isUndefined)
    }

search
  = 'search' oneOnly:' first'? type:word where:where as:as? .? space* createIfNeeded:'Create if not exists'? {
      if (as && as.endsWith('.')) {
        as = as.substr(0, as.length -1)
      }
      return _.omit({
        command: 'search',
        type: type,
        where: where,
        getFirst: oneOnly,
        as: as,
        createIfNeeded: createIfNeeded
      }, _.isUndefined)
    }

where
  = ' where ' json:json {
      return json
    }

space
  = ' ' / '\t'

word
  = space? w:(letter / [\*\._-])+ {
    const word = w.join('').trim()
    if (/^\d+$/.test(word)) {
      return +word
    } else {
      return word
    }
  }

json
  = jsonObject / jsonArray

jsonObject
  = '{' p:pair* '}' {
      var result = "{" + _(p).flatten().compact().value().join(', ') + "}"
      return JSON.parse(result)
    }

jsonArray
  = '[' list:(jsonOrWord ','?)* ']' {
    let val =  _.map(list, (value) => value[0])
    return val
  }

pair
  = key:word ':' space+ val:(word / string / json) ','?{
      return '"' + key + '"' + ': ' + val
    }

jsonOrWord
  = val:(json / (!'if' word)) {
    return _.isArray(val) && _.compact(val)[0] || val
  }


string
  = '"' w:word '"' {
    return '"' + w + '"'
  }

letter
  = [a-zA-Z0-9]

as
  = ' as' varName:word {
      return varName
    }

/**
    [
    'at ease',
    'get contentPersonRole where {contentId: this._id} as cpr',
    'get content audioToContent.Content_id with ',
    'update event content.Event_id,
    'push speaker to event.speakers',
    'push this.Language_id to event.languages',
    ]

**/
