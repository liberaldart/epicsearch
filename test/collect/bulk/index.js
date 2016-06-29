it('Bulk response', function(done){
  var results = es.bulk.collect({body: [{delete: {_type: config.default_type, _index: config.default_index, _id: '1'}}]});

  results.should.be.fulfilled.then((res) => {
    expect(res)
      .to.be.an('object')
      .and.to.contain.all.keys(["took", "errors", "items"])
      .with.property("items").to.be.an('array')
  }).should.notify(done);
});
