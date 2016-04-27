{
  'use strict'
  const _ = require('lodash')
}

start
  = statement

statement
  = get / search / async / memUpdate 

memUpdate
  = command:word val:word space 'in' doc:word space 'at' path:word {
      return {
        command: command,
        doc: doc,
        path: path,
        value: val
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
  = 'search' type:word where:where as:as? {
      return _.omit({
        command: 'search',
        type: type,
        where: where,
        as: as
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
    return w.join('').trim()
  }

json
  = '{' p:pair ','? ps:pair* '}' {
      var result = "{" + _([p, ps]).flatten().compact().value().join(', ') + "}"
      return JSON.parse(result)
    }

pair
  = key:word ':' space? val:(word / string / json) {
      return '"' + key + '"' + ': ' + val
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
