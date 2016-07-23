
/**An elasticsearch client in NodeJS with deep relationship managenent, a query dsl and performance enhancement

Continue to use elasticsearch in Nodejs as you would. On top you can...
* Manage relationships and dependencies between nodes of the graph
* Access it using a query DSL which looks like english (Chrome extension to be made during HH16)
* Performance enhancement
* Multi lingual storage and retrieval, including search
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
  .then((res) => {
    console.log(2, 'get', res)
  })
  .catch(console.log)**/

/**'search person where {'name.suggest': *text} as persons. Retrieve field speakers',
'async each persons.hits.hits as person',[
  'add every *person.speakers to *speakerIds'
]
'search speaker where {_id is one of *speakerIds} as speakers. Join with person(retrieve only name), primaryLanguages (retrieve only name), secondaryLanguages (retrieve only name)',



'search video where {event.venue.name is *X AND speakers.person.name is *Y}'
**/
'use strict'
const _ = require('lodash')

var EpicSearch = require('./index')
var es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')

//'search speakers where person.name = *text'
//const assignments = ['x is 2', 'x is 4 if *x is 2 ? Else 1', 'x is 1 if *x is 42 ? Else is 2']
const assignments = ['k is {d: "*x", *x: 4} if *x is empty. Else 22' ]
const get = ['get event 1', 'get session 1', 'get speaker 1']
const isEqual = ['2 is 4?', '2 is 2?', '*x is 2?', '*x is 2 strict?']

const isEmpty = ['*x is not empty']

const search = ['search first event where {x: 1} as event. Create if not exists', 'search event where {_id: 1} as event2']

const searchWithJoin = ['search test where {_id: 1} as test with fields a, b. Join relationA with fields c,d'] //Not implemented yet

const memUpdate = ['addToSet 1 at *y.arr']
const memUpdateNew = ['push 3 at *y.arr']

const link = ['link *speaker with *english as primaryLanguages']
const link2 = ['link *session with *speaker as speakers']

const asyncEachThenGet = [
  'async each *arr as i',
    [
      'get test *i',
    ]
]

const index = ['index *event', 'index *session', 'index *speaker', 'index *hindi', 'index *english']

const ctx = {
  x: [7],
  y: {arr: [1,2]},
  arr: [1, 2],
  a: {_type: 'a', _id: '12'},
  b: {_type: 'b', _id: '1'},
  m: {_type: 'a', _source: {}, _id: '2'},
  event: { _type: 'event', _source: {}},
  session: { _type: 'session', _source: {}},
  hindi: {_id: '1', _type: 'language', _source: {name: 'hindi'}},
  english: {_id: '2', _type: 'language', _source: {name: 'english'}},
  speaker: {_id: '1', _type: 'speaker', _source: {}}
}
const execute = es.dsl.execute.bind(es.dsl)

execute(['index *speaker', 'index *hindi', 'index *english', 'index *event', 'index *session'], ctx)//Create event
.then((res) => {//LINK event with session
  return execute('link *session with *event as event', ctx)
  /**.then((res) => {
    return execute('get session *session._id as session', ctx)
    .then((session) => {console.log('session after linking with event', session)})
  })
  .then((res) => {
    return execute('get event *event._id as event', ctx)
    .then((event) => {console.log('event after linking with session', JSON.stringify(event))})
  })**/
})
.then(() => {//Link speakers with session. 
  return execute('link *speaker with *session as sessions', ctx)
  .then((res) => {

    return execute(['get session *session._id as session', 'get speaker *speaker._id as speaker', 'get event *event._id as event'], ctx)
    .then((event) => {
      console.log('event after linking speaker with session', JSON.stringify(event))
      console.log('speaker after linking speaker with session', JSON.stringify(ctx.speaker))
      console.log('session after linking speaker with session', JSON.stringify(ctx.session))
    })
  })
})
.then(() => {//Link speakers with a language. SHould add language to the event
  return execute('link *speaker with *english as primaryLanguages', ctx)
  .then((res) => {
    return execute(['get event *event._id as event', 'get speaker *speaker._id as speaker'], ctx)
    .then((event) => {
      console.log('speaker after linking speaker with english', JSON.stringify(ctx.speaker))
      console.log('event after linking speaker with english', JSON.stringify(ctx.event))
    })
  })
})
.then(() => {//Link speakers with a language
  return execute('link *session with *hindi as primaryLanguages', ctx)
  .then((res) => {

    return execute(['get session *session._id as session', 'get speaker *speaker._id as speaker', 'get event *event._id as event'], ctx)
    .then((event) => {
      console.log('event after linking session with hindi', JSON.stringify(event))
      console.log('speaker after linking session with hindi', JSON.stringify(ctx.speaker))
      console.log('session after linking session with hindi', JSON.stringify(ctx.session))
    })
  })
})
.then(() => {
  return execute(['unset *speaker.primaryLanguages Do deep update.'], ctx)
  .then((res) => {
    return execute('get event *event._id as event', ctx)
    .then((event) => {
      console.log('event after unlinking speaker with languages', JSON.stringify(event))})
  })
})
.catch(console.log)

var testInstructions = [ //Get and search retrieve entity object(s) from in memory cache which in turn is flled from ES as each respective query happens for first time. These are mutable objects. The idea is to let them go through a process of multiple mutations in memory during the migration process. Once the old tables are processed, (only) the dirty entities in cache are flushed to ES.
  'get event *content.eventId',
  'search content-to-audio-channel where {contentId: "*content._id"} as cac. Retrieve fields audioChannelId',
  'async each cac.hits.hits as contentToAudioChannel',
    [
      //Handle event.langauges
      'get audiochannel *contentToAudioChannel._source.audiochannelId',
      'addToSet *audioChannel.languageId in event at path primaryLanguages',

      //Handle event.speakers/translators
      'speakerFields are {personId: "*audiochannel.speakerId", primaryLanguages: "*audiochannel.languageId"}',
      'speakerFields.translationType is *audioChannel.translationType if *audioChannel.translationType is not empty',

      'speakerType is speaker if *audioChannel.translationType is empty? Else is translator',
      'search first *speakerType where *speakerFields as speakerOrTranslator. Create if not exists',

      'relationship is speakers if *audioChannel.translationType is empty? Else is translators',
      'link *speakerOrTranslator with *event as *relationship'
    ],
]


/***
 *THINGS TO DO
Ayush - Support for multi depth join at storage level (denormalization)
Ayush - Support for multi lingual field in field def
AYush - Unlink flow (Removes joins and recalculates dependencies of newly disconnected left subgraphs)
Ayush - On update of a linked field, reflect it in the left subgraph
Ayush - Implement the new link through unionIn from rightNode to leftNodes (instead of current, left nodes get unionFrom right nodes)
Test update with join across deep paths
Ashu = Add cache to dsl



Generate ES index mappings though config (nested for relationships)
Support for non schema type in queries
Define web.search and web.read contexts in TOML
db migration: create schema for DL
Add fields and join in search DSL (low prio)

DONE fix function results for lib/deep
DONE Support for multi depth unionIn with name of field included
DONE Change from storing strings of foreign keys to objects with _id and own:true (of added directly to this entity, and is not union from another place)
DONE Fix resolveJoins based on issue number 31
DONE Test update with unionIn across deep paths
TEST UPDATE
PASS - If no field has changed in source entity, do not flush it

TEST UPDATE - Union in
* Test copy to relationship across one level of depth, two levels of depth, then 4 levels of depth (induction principal)
* PASS - Adding a new field to an array should update copyTo relationship
* PASS - Setting a field should update copyTo relationship

TEST UPDATE - Joins
* Test joined data management across two levels of depth, then 4 levels of depth
* * Working - AddToSet, Set, unset, push, pull

TEST UPDTAE - Union in


TEST CREATE -

TEST GET

TEST SEARCH



LOng term:
Should work without language
Should also take index along with type
Versioning and locking for transactions


TEST for deep update/link/create

Create event with session and session with event
On index event should have primaryLanguage set from session.speakers.primaryLanguages
 *
 *
 */
