module.exports = {
  fields: ['title', 'startingDate', 'endingDate', 'description'],
  primaryField: 'title',
  joins: [{
    fieldName: 'sessions',
    fields: ['title', 'description'],
    primaryField: 'title'
  }, {
    fieldName: 'classifications',
    fields: ['name'],
    primaryField: 'name'
  }, {
    fieldName: 'venues',
    fields: ['name'],
    primaryField: 'name'
  }, {
    fieldName: 'languages',
    fields: ['name'],
    primaryField: 'name'
  }]
}
