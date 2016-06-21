it('Index Response', (done) => {
  var results = es.index.collect({body: {url: '13s'}, id: '1', index: config.default_index, type: config.default_type});

  results.should.be.fulfilled.then((res) => {
    expect(res).to.be.an('object');
    expect(res).to.have.property('_type');
    expect(res).to.have.property('_index');
    expect(res).to.have.property('_id');
    expect(res).to.have.property('_version');
    expect(res).to.have.property('status');
    expect(res).to.have.property('created');
  }).should.notify(done);
});
