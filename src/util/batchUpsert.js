'use strict';

exports.__esModule = true;

var _typeof2 = require('babel-runtime/helpers/typeof');

var _typeof3 = _interopRequireDefault(_typeof2);

var _assign2 = require('lodash/assign');

var _assign3 = _interopRequireDefault(_assign2);

var _flatten2 = require('lodash/flatten');

var _flatten3 = _interopRequireDefault(_flatten2);

var _chunk2 = require('lodash/chunk');

var _chunk3 = _interopRequireDefault(_chunk2);

var _isArray2 = require('lodash/isArray');

var _isArray3 = _interopRequireDefault(_isArray2);

var _isNumber2 = require('lodash/isNumber');

var _isNumber3 = _interopRequireDefault(_isNumber2);

exports.default = batchUpsert;

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function batchUpsert(client, tableName, batch, updateFields) {
  var chunkSize = arguments.length <= 4 || arguments[4] === undefined ? 1000 : arguments[4];


  var _returning = void 0;
  var autoTransaction = true;
  var transaction = null;

  var getTransaction = function getTransaction() {
    return new _bluebird2.default(function (resolve, reject) {
      if (transaction) {
        return resolve(transaction);
      }

      client.transaction(resolve).catch(reject);
    });
  };

  var wrapper = (0, _assign3.default)(new _bluebird2.default(function (resolve, reject) {
    var chunks = (0, _chunk3.default)(batch, chunkSize);

    if (!(0, _isNumber3.default)(chunkSize) || chunkSize < 1) {
      return reject(new TypeError('Invalid chunkSize: ' + chunkSize));
    }

    if (!(0, _isArray3.default)(batch)) {
      return reject(new TypeError('Invalid batch: Expected array, got ' + (typeof batch === 'undefined' ? 'undefined' : (0, _typeof3.default)(batch))));
    }

    const fieldsUpdate = updateFields.map(fieldName => `\`${fieldName}\` = VALUES(\`${fieldName}\`)`).join()
    //Next tick to ensure wrapper functions are called if needed
    return _bluebird2.default.delay(1).then(getTransaction).then(function (tr) {
      return _bluebird2.default.mapSeries(chunks, function (items) {
        //return tr(tableName).insert(items, _returning);
        return client.raw( tr(tableName).insert(items, _returning).toQuery() + " ON DUPLICATE KEY UPDATE " + fieldsUpdate )
        ;
      }).then(function (result) {
        if (autoTransaction) {
          tr.commit();
        }

        return (0, _flatten3.default)(result);
      }).catch(function (error) {
        if (autoTransaction) {
          tr.rollback(error);
        }

        throw error;
      });
    }).then(resolve).catch(reject);
  }), {
    returning: function returning(columns) {
      _returning = columns;

      return this;
    },
    transacting: function transacting(tr) {
      transaction = tr;
      autoTransaction = false;

      return this;
    }
  });

  return wrapper;
}
module.exports = exports['default'];