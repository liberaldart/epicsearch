{
  'use strict'
  const _ = require('lodash')
}

start
  = statement

statement
  = get / search / async / index / booleanExpression / assignment / memUpdate

memUpdate
  = command:word val:word space 'in' doc:word space+ 'at' ' path'? path:word {
      return {
        command: command,
        doc: doc,
        path: path,
        value: val
      }
    }

assignment
  = assignee:word (' is' / ' are') space+ value:jsonOrWord conditionalAssignment:conditionalAssignment? {
    const res = {
      command: 'assignment',
      assignee: assignee,
      assignment: value[1] || value
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
  = left:word ' is' right:(!'\?' word) strict:' strict'? space+ '?' {
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
  = 'async' func:word args:(!" as" word)+ space 'as' as:word {
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
  = 'search' oneOnly:' first'? type:word where:where as:as? .? space+ createIfNeeded:'Create if not exists'? {
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
  = space+ w:(letter / [\*\._-])+ {
    const word = w.join('').trim()
    if (/^\d+$/.test(word)) {
      return +word
    } else {
      return word
    }
  }

json
  = '{' p:pair ','? ps:pair* '}' {
      var result = "{" + _([p, ps]).flatten().compact().value().join(', ') + "}"
      return JSON.parse(result)
    }

pair
  = key:word ':' space+ val:(word / string / json) {
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
