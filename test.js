var EpicSearch = require('./index')
var config = require('./config')
var es = new EpicSearch(config)

/**es.crudUpdate({
    type: 'event',
    _id: 'AVHDEPUuQpxtrIE1g6xi',
    update: {
      set: {
        title_english: 'vab'
      }
    }
  })**/

  es.queryParser.parse([
    'get test *id as ab',
    'get test *ab._id',
    'search test where {x: "*ab._source.x"} as xx'
  ], {id : 1})
  .then(function(res) {
    console.log(res)
  })
  .catch(console.log)
