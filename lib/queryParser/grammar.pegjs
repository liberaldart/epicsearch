{
  'use strict'
}

start
  = statement

statement
  = get / test

get
  = 'get' type:word id:word as:as? {
      return _.clean({
        command: 'get',
        type: type,
        id: id,
        as: as
      })
    }

test
  = 'test'

space
  = ' '

word 
  = space w:(letter / [\*\._])+ {
      return w.join('').trim()
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
