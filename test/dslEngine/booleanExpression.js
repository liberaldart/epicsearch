'use strict';

  const test = (testDesc, statement, expectedValue) => {

    expectedValue = testDesc.split('with')[1].split('should return')[1].trim()

    if(expectedValue == 'true')
      expectedValue = true
    else if(expectedValue == 'false')
      expectedValue = false
    else if(!isNaN(expectedValue))
      expectedValue = Number(expectedValue)
    else if(typeof expectedValue === 'string') {
      expectedValue = JSON.parse(expectedValue)
    }
    statement = [];
    statement.push(testDesc.split('with')[0].trim());

    const ctx = JSON.parse(testDesc.split('with ctx')[1].split('should return')[0].replace('=', '').trim());

    it(testDesc, (done) => {
      const result = es.dsl.execute(statement, ctx)

      result.should.be.fulfilled.then((res) => {
        //expect(res).to.be.a('boolean')
        if(typeof expectedValue === 'object')
          expect(res).to.deep.equal(expectedValue)
        else
          expect(res).to.be.equals(expectedValue)

      }).should.notify(done)
    })
  }

describe('boolean expressions : equality check', () => {
  test('2 is 4? with ctx = {} should return false')
  test('2 is 2? with ctx = {} should return true')
  test('*x is 2? with ctx = {"x" : 10} should return false')
  test('*y is 2 strict? with ctx = {"y" : 2} should return true')
  test('*z is 2 strict? with ctx = {"z" : "2"} should return false')
  test('*z is 2 strict? with ctx = {"z" : "2"} should return false')
})

describe('boolean expressions : empty check', () => {
  test('*x is not empty with ctx = {"x" : "10"} should return true')
  test('*x is empty with ctx = {"x" : 10} should return false')
  test('*y is empty with ctx = {"y" : null} should return true')
  test('*x is empty with ctx = {"x" : []} should return true')
  test('*x is empty with ctx = {} should return true')
});

describe('boolean expressions : assignment', () => {
  test('h is 3 with ctx = {} should return 3')
  test('h is *x with ctx = {"x" : 100} should return 100')
  test('k is {d: "*x", *x: 4} if *x is not empty. Else 22 with ctx = {"x" : 4} should return {"d" : 4, "4" : 4}')
  test('k is 100 if *x is ASHU strict?. Else 200 with ctx = {"x" : "ASHU"} should return 100')
})
