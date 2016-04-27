module.exports =   {
    clientParams:{
      hosts: [{'host': 'localhost', 'protocol': 'http', 'port': 9200}],
      requestTimeout: 90000,
      maxConnections: 200
    },
    /**cloneClientParams:{
      hosts: [{'host': 'ep-st1', 'protocol': 'http', 'port': 9200}],
      requestTimeout: 90000,
      maxConnections: 200
    },**/
    default_index: 'test',
    default_type: 'test',
    percolate:{
      query_index: 'queries'
    },
    batch_sizes: {
      mpu: 2,
      msearch: 2,
      index: 20,
      mget: 2,
      get: 2,
      bulk_index: 2,
      search: 1,
      bulk: 20//This is required as 1, since search internally
      //uses msearch and stripArrayResponses doesn'y work if timeout or size is not set
    },
    timeouts: {
      index: 1000,
      index_by_unique: 1000,
      get_first: 20,
      bulk_index: 1000,
      bulk: 10,
      get: 1000,
      bulk: 2000,
      mget: 1000,
      msearch: 1000
    },
    configBasePath: './config',
    configsToLoad: ['schema', 'web.read', 'web.search'],
    entities: ['entity'],
    languages: ['english', 'french']
}
