it('Index Response', (done) => {
  var results = es.index.collect({index: 'tests', type: 'test', id: 3, body: {field : 'Test 3'}});

  results.should.be.fulfilled.then((res) => {
    expect(res).to.be.an('object')
    .and.to.have.all.keys(['_type', '_index', '_id', '_version', 'status', 'created'])
  }).should.notify(done);
})
