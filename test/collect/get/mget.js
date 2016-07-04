it('First Response', (done) => {
  var results = es.mget.collect({index: 'tests', type: 'test', body: {ids: [1,2]}});

  results.should.be.fulfilled.then((res) => {
    expect(res)
    .to.be.an('object')
    .and.to.have.property('docs').to.be.an('array')
  }).should.notify(done);
});
