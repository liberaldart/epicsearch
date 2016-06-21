it('Get response', function(done){
  var results = es.get.collect({id: 1, index: config.default_index, type: config.default_type});
  results.should.be.fulfilled.then(function(res){
    expect(res).to.be.an('object');
    expect(res).to.have.property('_index');
    expect(res).to.have.property('_type');
    expect(res).to.have.property('_id');
    expect(res).to.have.property('_version');
    expect(res).to.have.property('found');
    expect(res).to.have.property('_source');
  }).should.notify(done);
});
