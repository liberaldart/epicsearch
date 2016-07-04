it('Multi Search Response', (done) => {
  var results = es.msearch.collect({body: [{search_type: 'count'},{query: {termff: {url: 1}}},{},{query: {term: {url: 2}}}]});

  results.should.be.fulfilled.then((res) => {
    expect(res).to.be.an('object')
    .and.to.have.all.keys(['responses'])
  }).should.notify(done);
});
