it('Search Response', (done) => {
  var results = es.search.collect({ query:{term:{url:13}}});

  results.should.be.fulfilled.then((res) => {
    expect(res).to.be.an('object');
    expect(res).to.have.property('took');
    expect(res).to.have.property('timed_out');
    expect(res).to.have.property('_shards');
  }).should.notify(done);
});
