
/**An elasticsearch client in NodeJS with deep relationship managenent, a query dsl and performance enhancement

Continue to use elasticsearch in Nodejs as you would. On top you can...
  
* Manage relationships and dependencies between nodes of the graph
* Access it using a query DSL which looks like english (Chrome extension to be made during HH16)
* Performance enhancement
* Role/rule based access management for crud operations

Test case coverage being done
**/
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

/**'search person where {"name.suggest": *text} as persons. Retrieve field speakers',
'async each persons.hits.hits as person',[
  'add every *person.speakers to *speakerIds'
]
'search speaker where {_id is one of *speakerIds} as speakers. Join with person(retrieve only name), primaryLanguages (retrieve only name), secondaryLanguages (retrieve only name)',



'search video where {event.venue.name is *X AND speakers.person.name is *Y}'
**/


var EpicSearch = require('./index')
var es = new EpicSearch('./newConfig')

//'search speakers where person.name = *text'
//const assignments = ['x is 2', 'x is 4 if *x is 2 ? Else 1', 'x is 1 if *x is 42 ? Else is 2']
const assignments = ['k is {d:"*x", *x: 4} if *x is empty. Else 22' ]

const isEqual = ['2 is 4?', '2 is 2?', '*x is 2?', '*x is 2 strict?']

const isEmpty = ['*x is not empty']

const search = ['search test where {_id: 1} as test']

const searchWithJoin = ['search test where {_id: 1} as test with fields a, b. Join relationA with fields c,d'] //Not implemented yet

const searchFirst = ['search first test where {x: "z"} as test', 'search test where {x: "jjm"} as test. Create if not exists']

const memUpdate = ['addToSet 3 in *y at path arr', 'addToSet 1 in *y at path arr']
const memUpdateNew = ['push 3 at *y.arr']

const link = ['link *a with *b as qelationAB']

const asyncEachThenGet = [
  'async each *arr as i',
    [
      'get test *i',
    ]
]

const index = ['index *y as type a', 'index *m']

var testInstructions = [
  'get event *content.eventId',
  'search content-to-audio-channel where {contentId: "*content._id"} as cac. Retrieve fields audioChannelId',
  'async each cac.hits.hits as contentToAudioChannel',
    [
      //Handle event.langauges
      'get audiochannel *contentToAudioChannel._source.audiochannelId',
      'addToSet *audioChannel.languageId in event at path primaryLanguages',

      //Handle event.speakers
      'speakerFields are {personId: "*audiochannel.speakerId", primaryLanguages: "*audiochannel.languageId"}',
      'speakerFields.translationType is *audioChannel.translationType if *audioChannel.translationType is not empty',
      'speakerType is speaker if *audioChannel.translationType is empty? Else is translator',
      'search first *speakerType where *speakerFields as speakerTypeEntity. Create if not exists',
      'link speaker with event as speakers'
    ],
]

const ctx = {x: [7], y: {arr: [1,2]}, arr: [1, 2], a: {_type: 'a', _id: "1"}, b: {_type: 'b', _id: "1"}, m: {_type: "a", _source: {}, _id: "2"}}
es.dsl.execute(index, ctx)
.then(function(res) {
  console.log(JSON.stringify(res), ctx.y)
})
.catch(console.log)


/***
 *THINGS TO DO
Add fields and join in search DSL (low prio)
DONE module that takes toml files and returns jsons from them
DONE new schema syntax in toml
grammar: update update
grammar: add linking
deep/update or create: allow triggers
if link is already there, dont resave that entity.
db migration: create schema for DL
dbMigration: create triggers
cached Execution: cache search results, especially docs (by id) in memory
dbMIgration: allow cached exuction so that multiple updates are merged in a single doc. The cache is flushed in db at end. FOr performance
db migration: start inserting tables in the database and run triggers to set data dependencies
relationship management file
  speakers <> events
    event <> speaker
    one to many
    add speaker.primaryLanguages to event.primaryLanguages

 *
 *
 */



