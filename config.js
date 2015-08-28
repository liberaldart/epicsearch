module.exports =   {
    clientParams:{
      hosts : [{'host': 'localhost', 'protocol': 'http', 'port': 9200}],
      requestTimeout: 90000,
      maxConnections: 200
    },
    /**cloneClientParams:{
      hosts : [{'host': 'ep-st1', 'protocol': 'http', 'port': 9200}],
      requestTimeout: 90000,
      maxConnections: 200
    },**/
    default_index : 'test',
    default_type: 'test',
    percolate:{
      query_index: 'queries'
    },
    resultContainers: {
      msearch: 'responses'
    },
    batch_sizes: {
      mpu: 2,
      msearch: 2, 
      mget: 2,
      get: 10,
      bulk_index: 2,
      search: 1//This is required as 1, since search internally 
      //uses msearch and stripArrayResponses doesn'y work if timeout or size is not set 
    },
    timeouts: {
      index_by_unique: 1000,
      get_first: 20,
      bulk_index: 1000,
      get: 1000,
      mget: 1000,
      msearch: 1000
    }
  }
