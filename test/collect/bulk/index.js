it('Bulk response', function(done){
  var results = es.bulk.collect({body: [{delete: {_index: 'tests', _type: 'test', _id: '1'}}, {delete: {_index: 'tests', _type: 'test', _id: '2'}}]});

  results.should.be.fulfilled.then((res) => {
    expect(res)
      .to.be.an('object')
      .and.to.contain.all.keys(["took", "errors", "items"])
      .with.property("items").to.be.an('array')
    })
  .catch((res) => {
  })
  .should.notify(done);
});
