it('Bulk response', function(done){
  var results = es.bulk.collect({body: [{delete: {_type: config.default_type, _index: config.default_index, _id: '2'}}]});

  results.should.be.fulfilled.then((res) => {
    expect(res).to.be.an('object');
    expect(res).to.have.property('took');
    expect(res).to.have.property('errors');
    expect(res).to.have.property('items');
    expect(res.items).to.be.an('array');
  }).should.notify(done);
});
