'use strict'
const debug = require('debug')('crudLink')
const async = require('async-q')
const _ = require('lodash')
const Q = require('q')
const await = require('asyncawait/await');
const suspendable = require('asyncawait/async');

const utils = require('./utils')
const Cache = require('../cache')
const graph = require('./graph')
/**
 * Link entities of a particular type, along a particular relationship,
 * with entities of possibly different types. The linking goes through provided:
 * a. Schema.e1.e1Relation definition exists
 * b. All e2 types are allowed as per schema.e1.e1Relation definition
 * c. If the reverse relationship name is specified in schema.e1.e1Relation,
 *    and is specified in e2 schema,
 *    and it carries e1Relation as the reverse name under which e1Type is valid destination
 *
 * If the operation is not allowed for any combination of e1+e2, then an exception is thrown
 * immediately without applying any changes anywhere.

 * @params {Entity} e1
 * @params {String} e1ToE2Relation
 * @params {Entity or Array of entities} (e2 or e2Entities) single entity or array of objects with _type and _id fields
 *
 */
function Index(es) {
  this.es = es
}

Index.prototype.execute = function(params, cache) {
  const es = this.es
  const config = this.es.config
  let indexEntitiesAtEnd = false
  if (!cache) {
    cache = new Cache(es)
    indexEntitiesAtEnd = true
  }
  const entities = _.flatten([params.e2 || params.e2Entities])
  return async.eachSeries(entities, (e2) => {
    return dualLink(params.e1, e2, params.e1ToE2Relation, cache, config)
  })
  .then(() => {
    if (indexEntitiesAtEnd) {
      return cache.flush()
    } else {
      return Q()
    }
  })
  .then(() => {
    return suspendable(() => {
      const res = {
        e2ToE1Relation: params.e1ToE2Relation,
        e1: await(utils.getEntity(cache, params.e1._id, params.e1._type))
      }
      if (params.e2) {
        res.e2 = await(utils.getEntity(cache, params.e2._id, params.e2._type))
      } else {
        res.e2Entities = await(params.e2Entities.map((e2) => {
          return utils.getEntity(cache, e2._id, e2._type)
        }))
      }
      return res
    })()
  })
  .catch((err) => {
    debug(err, err.stack)
    throw err
  })
}

const dualLink = (e1, e2, e1ToE2Relation, cache, config) => {
  const e1ToE2RelationDef = config.schema[e1._type][e1ToE2Relation]
  if (!e1ToE2RelationDef) {
    throw new Error('Relation not found: EntityType: ' + e1._type + '. Relation: ' + e1ToE2Relation)
  }
  const e2ToE1Relation = e1ToE2RelationDef.inName

  return Q.all([
    makeLink(e1, e1ToE2Relation, e2, cache, config),
    makeLink(e2, e2ToE1Relation, e1, cache, config)]
  )
  /**.spread((res1, res2) => {
    if (res1 != 'already linked' && res2 != 'already linked') {
      debug('just dual linked', e1._type, e1ToE2Relation, e2._type)
    }
  })**/
}

const makeLink = (e1, e1ToE2Relation, e2, cache, config) => {
  if (!e1ToE2Relation) {
    return Q()
  }

  return isAlreadyLinked(e1, e2, e1ToE2Relation, cache)
  .then((isAlreadyLinked) => {
    if (isAlreadyLinked) {//Don't need to link
      //debug('already linked', e1._type, e1ToE2Relation, e2._type)
      return Q('already linked')
    } 

    return setRelationPropertyInE1(cache, e1, e1ToE2Relation, e2)
    .then(() => {
      return graph.updateLeftSubgraph(cache, e1, [e1ToE2Relation, e2._id], e2, 5)
    })
  })
}

const isAlreadyLinked = (e1, e2, e1ToE2Relation, cache) => {
  return utils.getEntity(cache, e1._id, e1._type)
  .then((e1Full) => {
    return _.find(_.flatten([e1Full._source[e1ToE2Relation]]), _.pick(e2, '_id'))
  })
}

const setRelationPropertyInE1 = (cache, e1, e1ToE2Relation, e2) => {

  const e1Schema = cache.es.config.schema[e1._type]
  const relationSchema = e1Schema[e1ToE2Relation]
  if (!relationSchema) {
    throw new Error('makeLink: ' + e1ToE2Relation + ' not found in ' + e1._type + ' schema')
  }
  const linkOp = _.isArray(relationSchema.type) && 'addToSet' || 'set'
  //debug(e1._type, e1ToE2Relation, linkOp, JSON.stringify(_.pick(e2.fields, [e1ToE2Relation])))
  return cache.es.deep.update({
    _type: e1._type,
    _id: e1._id,
    update: {
      [linkOp]: {
        [e1ToE2Relation]: {_id: e2._id}
      }
    }
  }, cache)
}

//Update fields and relations of e2 and its graph, based on 
//their newly discovered unionFrom, copyFrom and joinFrom paths through e1 vertex
const updateV1GraphFromV2Graph = (cache, currentVertex, currentToV1RelationPath, v1, v1ToV2Relation, v2) => {
  const e1Schema = cache.es.config.schema[e2._type]
  //For every field of e2 schema
  return async.eachSeries(_.keys(e1Schema), (e1Field) => {
    
    //Find new paths established through e1
    let e1FieldUnionFromPaths = e2Schema[e1Field].unionFrom
    if (!e1FieldUnionFromPaths) {
      return Q()
    }
    e1FieldUnionFromPaths = _.filter(e1FieldUnionFromPaths, (unionFromPath) => _.includes(_.dropRight(unionFromPath, 1), e2ToE1Relation))
    
    //Upon finding such paths
    if (e1FieldUnionFromPaths.length) { 
      //For each such path
      return async.eachSeries(e1FieldUnionFromPaths, (e1FieldUnionFromPath) => {
        
        //Find paths going from E1, the vertices at the end of which
        //have fields which will get unioned in e2.e1Field 
        const pathFromE1ToEntitiesToUpdateFrom = _(e1FieldUnionFromPath).drop(1).dropRight(1).value()
        const e1FieldToUnionInE2Field = _.last(e1FieldUnionFromPath)

        //Get entities at that path, information from which, is to be unioned in e1Field
        return suspendable(() => {
          let entitiesToUnionFrom = await(
            utils.getEntitiesAtPath(cache, e1, pathFromE1ToEntitiesToUpdateFrom)
          )
          //entitiesToUnionFrom = _.flatten([entitiesToUnionFrom])
          return async.eachSeries(entitiesToUnionFrom, (entityToUnionFrom) => {
            return utils.recalculateUnionInSibling(null, entityToUnionFrom, e1FieldToUnionInE2Field, e2, e1Field, cache)
          })
        })()
      })
    }
  })
}

module.exports = Index

if (require.main === module) {
  const EpicSearch = require('../../index')
  const es = new EpicSearch('/home/master/work/code/epicsearch/newConfig')
  const Cache = require('../cache')
  const cache = new Cache(es)
  debug(JSON.stringify(cache.es.config.schema))
  es.deep.link({
    e2: {
      _type: 'session',
      _id: '1'
    },
    e1: {
      _type: 'event',
      _id: '1'
    },
    e1ToE2Relation: 'sessions'
  }, cache)
  .then(function(res) {
    debug('cache data', JSON.stringify(cache.data))
    debug('res', JSON.stringify(res))
    return cache.flush()
  })
  /**.then(() => {
    return es.dsl.execute(['get session 1'])
  })
  .then((res) => {debug(JSON.stringify(res))})
  .then(() => {
    return es.dsl.execute(['get event 1'])
  })
  .then((res) => {debug(JSON.stringify(res))})
  .then(() => {
    return es.dsl.execute(['get speaker 1'])
  })
  .then((res) => {debug(JSON.stringify(res))})**/
  .catch(debug)
}
