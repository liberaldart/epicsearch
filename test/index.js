require('./common.js');

describe('testing epicsearch', function() {

  this.timeout(15000);

  describe('collect API', () => {
    require('./collect/aggregator.js');
    require('./collect/get/index');
    require('./collect/get/mget');
    require('./collect/search/index');
    require('./collect/search/multi_search');
    require('./collect/bulk/index');
    require('./collect/index/index');
  });

  describe('DSL API', () => {
    require('./dslEngine/resolve');
    require('./dslEngine/booleanExpression')
  });

  describe('deep API', () => {

  });
});
