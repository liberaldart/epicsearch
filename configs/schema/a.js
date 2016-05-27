module.exports = {
  a: {
    type: [String]
  },
  relA: {
    isRelationship: true,
    revName: 'relB',
    type: 'b'
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

