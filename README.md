
#Setup:
1. Run elasticsearch deamon in a docker:
  a) Run this command to allow bootstrapping successfully on host:
    sudo sysctl -w vm.max_map_count=262144
  b) Copy "Dockerfile" content from : https://raw.githubusercontent.com/docker-library/elasticsearch/8347a4697d32878dbacd7086b34f76a9e19113ae/2.3/Dockerfile
  into a new directory.
  c) Run: docker run -d elasticsearch
This will run Elasticsearch deamon in a new docker.
You would need to commit the running docker. You can google it.
2. 
#Elasticsearch in nodejs ++

Request batching for heavy load performance optimization and some useful utlity methods added on top of Elasticsearch's official nodejs module v ^10.0.1

###Installation

npm install epicsearch

###Setup

```
var epicsearch = require('epicsearch')
var es = new epicsearch(config) 
//These two lines replace your require('elasticsearch') and new elasticsearch.client(config) calls
```

The config will look something like this
```Javascript
  { 
    clientParams: {
      hosts: [{'host': 'localhost', 'protocol': 'http', 'port': 9200}],
      requestTimeout: 90000,
      maxConnections: 200
    },
    cloneClientParams:{ 
      hosts : [{'host': 'ep-st1', 'protocol': 'http', 'port': 9200}],
      requestTimeout: 90000,
      maxConnections: 200
    },
    percolate:{
      query_index: 'queries'
    },
    batch_sizes: {
      mpu: 2,
      msearch: 50, 
      index: 50,
      mget: 10,
      get: 10,
      bulk_index: 50,
      search: 50
    },
    timeouts: {
      index: 2000,
      index_by_unique: 2000,
      getFirst: 2000,
      bulk_index: 1000,
      get: 2000,
      mget: 2000,
      search: 2000,
      msearch: 2000
    }
  }
```
From here on, you can use the epicsearch 'es' client instance, as you would have used elasticsearch module in your code. Epicsearch is first a wrapper around Elasticsearch module and, it provides some added features on top. For all elasticsearch module supported methods, it will simply delegate the calls to embedded elasticsearch module. If you are already using elasticsearch, you will see no change anywhere, whether in code or in es requests form/flow. Once you start using any epicsearch specific features (mentioned below), then epicsearch will come into play.

###PERFORMANCE FEATURE
####Bulk batching of queries for much better performance

Aggregate multiple requests (of same kind) from different places in your application logic, for better performance under heavy search/get/index/bulk query load. The requests will be collected till either the bulk_size or timeout threshold for that request are breached. Once the threshold is crossed, the requests are flushed to Elasticsearch backend in one bulk request. The value of bulk_size or timeout can be set in passed config at client creation time. This is a significant performance optimization when you are making hundreds of independent (but same kind of ) queries in parallel.

In order to use this query aggregation, just append .agg to your existing elasticsearch-js call. 

For example, 
```
es.get.agg({index:"test", type: "test", id: "1"}).then() //Notice the .agg there? That is all you have got to do

es.{method}.agg(esMethodParams)
``` 
Request and response format is designed to be same as ES. 

**Currently supporting batching for methods:** 
* index
* index_by_unique
* get_first
* bulk_index
* get
* mget
* search
* msearch

###FUNCTIONAL FEATURES

####get_first

Elasticsearch does not support uniqueness constraint. In case your store tends to accumulate duplicates over a unique key over time, the primary document for that key can be identified (by applying a sort or even without it). This function returns that doc, and also the count of docs matching given key/val in the index/type supplied.

```
/**
 * Different version that native elasticsearch get. It returns
 * first document matching a particular key === value condition.
 * If not found, it returns {total: 0}. Uses search with sort internally.
 *
 *@param index The index to search on. Default is config.default_index
 *@param type The type of document to be matched. Default is epicdoc
 *@param key The field on which to do term query to get first document matching it
 *@param val The value to match for given key
 *@param match Optional. The type of match to do like match_phrase. If not specified, term match is done
 *@param sort How to sort the duplicates for key:  val
 *@param fields array of stored fields to fetch from ES object. Optional. If not specified the whole object is returned
 */
  es.get_first({
    index: 'infinity', 
    type: 'members', 
    key: 'tags', 
    val: ['silv3r','vaibhav'],
    sort: {memberSince: 'desc'},
    fields: ['profileUrl']
  ).then(function(res) {
    console.log(res)
  })
  
  /**
  Response
  [{ doc: { profileUrl: 'http://github.com/mastersilv3r', _id: '1' }, total: 2 }, {total: 0}]
  **/

```

####index_by_unique

 Workaround for lack of unique id limitation of Elasticsearch. 
 This helps you index (or override existing) docs based on 
 "unique ids" stored in the key field. Uses get_first internally to 
 get the first documents for given key, and then overwrites those 
 documents with supplied docs

```
/**
 *
 *@param index The index to insert in
 *@param type The type of document to be inserted
 *@param doc|docs the doc(s) to be saved. Can be array or single element. Response is array or single element accordingly
 *@param key The unique field by which to do matches and get 'first' document from ES to overwrite (if it exists)
 *@param sort Optional. In case duplicates exist, the one to be on top of sort result will be overwritten by the input doc. If this is not specified, the document that gets overwritten is arbitrarily chosen.
 **/


  es.index_by_unique({
    index: 'test',
    type: 'test',
    docs: [{ url: '13', sortField: 23}, {url: '13', sortField: 24}]
    key: 'url',
    sort: {sortField: 'desc'},
    match: 'match_phrase',
  })
  .then(function(res) {
    debug('indexed by uniqueness', res)
  })

/**
indexed by uniqueness 
  [{ index: 
     { _index: 'test',
       _type: 'test',
       _id: 'd77WvroaTruYJ-3MdJ6TXA',
       _version: 3,
       status: 200 } },
  { index: 
     { _index: 'test',
       _type: 'test',
       _id: 'd77WvroaTruYJ-3MdJ6TXA',
       _version: 4,
       status: 200 } 
  }]
**/

```

####bulk_index
Shorter expression for bulk indexing. If you use cloneClientParams in config, this will also flush the bulk indexes to the other destination.

```
  es.bulk_index({
    docs: [{url: 13},{url: '1233r', _id: 1}], 
    index: 'test', 
    type: 'test'
  })
  .then(debug)

/**
Response:
{"took":4,"errors":true,"items":[{"create":{"_index":"test","_type":"test","_id":"tPCohIFzQxO5jPdYHBdWIw","_version":1,"status":201}},{"index":{"_index":"test","_type":"test","_id":"1","status":400,"error":"MapperParsingException[failed to parse [url]]; nested: NumberFormatException[For input string: \"1233r\"]; "}}]}
**/

```

####get_dups

```
/**
 * Returns values for a given key which have duplicates, with count of docs for each value
 *
 * @param index
 * @param type
 * @param key the key by which to find dups
 * @param size Optional. how many dups you want. Default is 10
 * @param shardSize Optional. Since aggregation is used inside. You may want a high enough value of shardSize for more accuracy of duplicate counts. Default is 1000000
 */

  es.get_dups({key: 'url', index: 'test', type: 'test'})
  .then(debug)

/**
Response
 [ { val: 13, doc_count: 2 } ]
**/
```

#### Multi Percolate and Update
Use this to match any documents with stored percolate queries. Each query has an update logic set in it. For all queries matched,
the input document is updated as per the update logic in the query. 
You will find more details in the lib/percolate/mpu and lib/mem/get and lib/mem/update files. 
I need to consolidate the documentation for this. If you really need to use this urgently, then raise an issue.

####This is it for now
**If you want any more features, send me a pull request or raise an issue**

####BEING MADE WITH LOVE
**In the Himalayas, at [Ghoomakad hackerspace @ Infinity](http://hackerspaces.org/wiki/infinity)**
