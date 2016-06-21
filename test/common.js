global.chai = require('chai');
chai.use(require('chai-as-promised'));
global.should = chai.should();
global.expect = chai.expect;
global.assert = chai.assert;

var EpicSearch = require('../index')

global.config = require('../config')
global.es = new EpicSearch(config)
