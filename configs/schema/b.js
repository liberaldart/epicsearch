module.exports = {
  a: {
    type: [String]
  },
  relB: {
    isRelationship: true,
    type: 'a',
    revName: 'relA'
  }
}

// entity:{
//   triggers: {},
//   fields: {},
//   relations: {
//     parent: {
//       type: ['person'],
//       revName: 'child',
//       copyTo: ['relationName.rel3.rel4.fieldA', 'relation2Name'],
//       unionTo: ['relation2Name'],
      
//     }
//   },

// }

