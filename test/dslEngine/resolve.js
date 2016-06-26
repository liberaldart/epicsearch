'use strict';

const resolve = require('../../lib/dslEngine/resolve');

it('Parse the assignment statement and return assigned value', (done) => {

  const ctx = {};
  var result = es.dsl.execute(['h is 3'], ctx);

  result.should.be.fulfilled.then((res) => {
    expect(res).to.be.an('number');
    expect(resolve('*h', ctx)).to.be.equals(3);
  }).should.notify(done);
});


it('Parse the conditional statement and return the assigned value, false case', (done) => {
  const ctx = {};

  var result = es.dsl.execute(['k is {d: "*x", *x: 4} if *x is empty. Else 22'], ctx);

  result.should.be.fulfilled.then((res) => {
    expect(res).to.be.an('object');
    expect(res).to.have.property('d');
    expect(res).to.have.property('10');
    expect(ctx).to.have.property('k');
  }).should.notify(done);
});
