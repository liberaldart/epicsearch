
/**
* phrases: array
* field: which field to match
* op: must, should or must_not
**/
module.exports.make = function(phrases, field, op) {
    var phrase_queries = phrases.map(function(phrase) {
       var q = {
            match_phrase:{} 
       }
       q.match_phrase[field] = phrase
       return q
    })
    var boolQ = {
        bool:{}
    }
    boolQ.bool[op] = phrase_queries

    return boolQ
}
