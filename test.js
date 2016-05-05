var EpicSearch = require('./index')
var config = require('./config')
var es = new EpicSearch(config)

/**es.update.deep({
    _index: 'test',
    _type: 'test',
    _id: '1',
    context: {
      fields: ['a']
    },
    lang: 'english'
  })
  .then(function(res) {
    console.log(2, 'get', res)
  })
  .catch(console.log)**/
/**es.queryParser.parse([
  'async each *ids as ida',[
    'get test *ida as idaTest',
    'async each *ids as id',[
      'get test *id as x',
      'addToSet *idaTest._id in *x at _source.y'
    ]
  ]
], {ids: [1, 2]})
.then(console.log)
.catch((e) => {
  console.log(e, e.stack)
})**/

/**'search person where {"name.suggest": *text} as persons. Retrieve field speakers',
'async each persons.hits.hits as person',[
  'add every *person.speakers to *speakerIds'
]
'search speaker where {_id is one of *speakerIds} as speakers. Join with person(retrieve only name), primaryLanguages (retrieve only name), secondaryLanguages (retrieve only name)',


'get videos where speaker.person.name = Dalai and event.venue.name = tipa'
'search speakers where person.name = *text'

'search video where {event.venue.name is *X AND speakers.person.name is *Y}'
{
  type: video,
  fields: {
    processingStatus: 'edit'
  },
  relationships: {
    event: {
      relationships: {
        venue: {
          fields: {
            name: X
          }
        }
      },
      fields: {
        startingDate: {
          gt: 44,
          lt: 55
        }
      }
    },
    speakers: {
      relationships: {
        person: {
          fields: {
            name: Y
          }
        }
      }
    }
  }
}
**/




//const assignments = ['x is 2', 'x is 4 if *x is 2 ? Else 1', 'x is 1 if *x is 42 ? Else is 2']
const assignments = ['k is {d:"*x", *x: 4} if *x is empty. Else 22' ]
const isEqual = ['2 is 4?', '2 is 2?', '*x is 2?', '*x is 2 strict?']
const isEmpty = ['*x is not empty']
const search = ['search test where {_id: 1} as test']

const asyncEachThenGet = [
  'async each *arr as i',
    [
      'get test *i',
    ]
]
var testInstructions = [
  'get event *content.eventId',
  'search content-to-audio-channel where {contentId: "*content._id"} as cac. Retrieve fields audioChannelId',
  'async each cac.hits.hits as contentToAudioChannel',
    [
      'get audiochannel *contentToAudioChannel._source.audiochannelId',

      'speakerType is speaker if *audioChannel.translationType is empty? Else is translator',

      'speakerFields are {personId: "*audiochannel.speakerId", primaryLanguages: "*audiochannel.languageId"}',
      'speakerFields.translationType is *audioChannel.translationType if *audioChannel.translationType is not empty',

      'search *speakerType with *speakerFields as *speakerType. Create if needed',

      'add *speaker._id to *event.speakers if not there already',
      'add *audioChannel.languageId to *event.primaryLanguages if not there already',
    ],
  'index event'
]

es.queryParser.parse(search, {x: [7], arr: [1, 2]})
.then(function(res) {
  console.log(JSON.stringify(res))
})
.catch(console.log)
