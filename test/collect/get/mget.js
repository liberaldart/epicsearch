it('First Response', (done) => {
  var results = es.mget.collect({index: config.default_index, type: config.default_type, body: {ids: [1,2]}});

  results.should.be.fulfilled.then((res) => {
    expect(res).to.be.an('object');
    expect(res).to.have.property('docs');
    expect(res.docs).to.be.an('array');
  }).should.notify(done);
});
