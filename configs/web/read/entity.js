module.exports = {
  fields: ['title', 'description', 'startingDate', 'endingDate', 'city', 'state', 'country'],
  primaryField: 'title',
  joins: [{
    fieldName: 'sessions',
    fields: ['title', 'description'],
    primaryField: 'title',
    joins: [{
      fieldName: 'languages',
      fields: ['name'],
      primaryField: 'name'
    }]
  }, {
    fieldName: 'venues',
    fields: ['name'],
    primaryField: 'name'
  }, {
    fieldName: 'classifications',
    fields: ['name'],
    primaryField: 'name'
  }, {
    fieldName: 'languages',
    fields: ['name'],
    primaryField: 'name'
  },
  {
    fieldName: 'speakers',
    primaryField: 'person.name',
    fields: ['languages.name'],
    joins: [{
      fieldName: 'person',
      primaryField: 'name',
      fields: ['name']
    }, {
      fieldName: 'languages',
      fields: ['name'],
      primaryField: 'name'
    }]
  }]
}
