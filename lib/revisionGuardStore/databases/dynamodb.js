'use strict';

var util = require('util'),
  Store = require('../base'),
  _ = require('lodash'),
  uuid = require('uuid/v1'),
  async = require('async'),
  debug = require('debug')('revisionGuard:dynamodb'),
  ConcurrencyError = require('../../errors/concurrencyError'),
  aws = Store.use('aws-sdk');

function DynamoDB(options) {
  Store.call(this, options);

  var defaults = {
    revisionTableName: 'revision',
    ttl:  1000 * 60 * 60 * 1, // 1 hour
    endpointConf: {
      endpoint: 'http://localhost:4567' //dynalite
    },
    // heartbeat: 1000
  };
  if (process.env['AWS_DYNAMODB_ENDPOINT']) {
    defaults.endpointConf = process.env['AWS_DYNAMODB_ENDPOINT'];
  }
  debug('region', aws.config.region);
  if(!aws.config.region){
    aws.config.update({region: 'us-east-1'});
  }
  _.defaults(options, defaults);
  this.options = options;
}

util.inherits(DynamoDB, Store);

_.extend(DynamoDB.prototype, {

  connect: function (callback) {
    var self = this;
    self.client = new aws.DynamoDB();
    debug('Client created.');
    function revisionTableDefinition(opts) {
      var def = {
        TableName: opts.revisionTableName,
        KeySchema: [
          { AttributeName: 'id', KeyType: 'HASH' },
        ],
        AttributeDefinitions: [
          { AttributeName: 'id', AttributeType: 'S' },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: opts.EventsReadCapacityUnits || 5,
          WriteCapacityUnits: opts.EventsWriteCapacityUnits || 5
        }
      };

      return def;
    }
    createTableIfNotExists(self.client, revisionTableDefinition(self.options), function (err) {
      if (err) {
        console.error('connect error: ' + err);
        debug('createTableIfNotExists', err);
        if (callback) callback(err);
      } else {
        self.emit('connect');
        self.isConnected = true;
        debug('Connected.');

        if (self.options.heartbeat) {
          self.startHeartbeat();
        }
        if (callback) callback(null, self);
      }
    });
  },

  disconnect: function (callback) {
    this.emit('disconnect');
    debug('Disconnected.');
    if (callback) callback(null);
  },

  getNewId: function(callback) {
    callback(null, uuid());
  },

  get: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      debug('get err', id, err);
      return callback(err);
    }

    var params = {
      TableName : this.options.revisionTableName,
      Key: {
        id: {S: id}
      },
      ReturnConsumedCapacity: 'TOTAL',
    };

    this.client.getItem(params, function (err, data) {
      debug('get params', JSON.stringify(params, null, 2));
      debug('get params', params);
      debug('get err', err);
      debug('get data', JSON.stringify(data, null, 2));
      if (err) {
        return callback(err);
      }

      if (!data|| !data.Item|| !data.Item.revision) {
        return callback(null, null);
      }

      callback(null, data.Item.revision.S || null);
    });
  },

  set: function (id, revision, oldRevision, callback) {
    if (!id || !_.isString(id)) {
      var errId = new Error('Please pass a valid id!');
      debug('set err', id, errId);
      return callback(errId);
    }
    if (!revision || !_.isNumber(revision)) {
      var err = new Error('Please pass a valid revision!');
      debug('set err', id, err);
      return callback(err);
    }

    var self = this;
    async.waterfall([
      function(done){
        self.get(id, done);
      },
      function(storeRevision, done) {
        if(storeRevision && storeRevision !== _.toString(oldRevision)){
          var err = new ConcurrencyError();
          debug('set ConcurrencyError', err);
            return done(err);
        }
        var params = {
          TableName : self.options.revisionTableName,
          Item: {
            id: {S: id},
            revision: {S: _.toString(revision)}
          },
          ReturnConsumedCapacity: 'TOTAL',
        };
        debug('set params', JSON.stringify(params, null, 2));
        self.client.putItem(params, done);
      },
    ], function (err, data) {
      debug('set err', err);
      debug('set data', JSON.stringify(data, null, 2));
      if (callback) {
        callback(err);
      }
    });
  },

  saveLastEvent: function (evt, callback) {
    var params = {
     Item: {
       id: {S: 'THE_LAST_SEEN_EVENT'},
       revision: {S: JSON.stringify(evt)}
     },
     TableName: this.options.revisionTableName,
     ReturnConsumedCapacity: 'TOTAL',
    };
    this.client.putItem(params, function (err, data) {
      debug('saveLastEvent params', JSON.stringify(params, null, 2));
      debug('saveLastEvent err', err);
      debug('saveLastEvent data', JSON.stringify(data, null, 2));
      if (callback) {
        callback(err);
      }
    });
  },

  getLastEvent: function (callback) {
    var params = {
     Key: {id: {S: 'THE_LAST_SEEN_EVENT'}},
     TableName: this.options.revisionTableName
    };
    this.client.getItem(params, function (err, data) {
      debug('getLastEvent params', JSON.stringify(params, null, 2));
      debug('getLastEvent err', err);
      debug('getLastEvent data', JSON.stringify(data, null, 2));
      if (err) {
        return callback(err);
      }

      if (!data || !data.Item || !data.Item.revision) {
        return callback(null, null);
      }

      callback(null, JSON.parse(data.Item.revision.S) || null);
    });
  },

  clear: function (callback) {
    var self = this;
    async.waterfall([
      function(done){
        self.client.scan({ TableName: self.options.revisionTableName }, done);
      },
      function(data, done){
        if(!data || !data.Items || data.Count === 0) {
          return done();
        }
        async.each(data.Items, function(entry, next){
          debug('delete item', JSON.stringify(entry, null, 2));
          var params = {
            Key: {
              id: {S: entry.id.S}
            },
            TableName: self.options.revisionTableName,
            ReturnConsumedCapacity: 'TOTAL'
          };
          self.client.deleteItem(params, next);
        }, done);
      }
    ], callback);
  },

  stopHeartbeat: function () {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      delete this.heartbeatInterval;
    }
  },

  startHeartbeat: function () {
    var self = this;

    var gracePeriod = Math.round(this.options.heartbeat / 2);
    this.heartbeatInterval = setInterval(function () {
      var graceTimer = setTimeout(function () {
        if (self.heartbeatInterval) {
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (dynamodb)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      waitForTableExists(self.client, self.options.revisionTableName, function (err) {
        if (graceTimer) clearTimeout(graceTimer);
        if (err) {
          console.error(err.stack || err);
          self.disconnect();
        }
      });
    }, this.options.heartbeat);
  },

});

var waitForTableExists = function(client, tableName, callback) {
  debug('Wating for table:', tableName);
  var params = {
    TableName: tableName
  };
  client.waitFor('tableExists', params, callback);
};

var createTableIfNotExists = function (client, params, callback) {
  var exists = function (p, cbExists) {
    client.describeTable({ TableName: p.TableName }, function (err, data) {
      if (err) {
        if (err.code === 'ResourceNotFoundException') {
          cbExists(null, { exists: false, definition: p });
        } else {
          console.error('Table ' + p.TableName + ' doesn\'t exist yet but describeTable: ' + JSON.stringify(err, null, 2));
          cbExists(err);
        }
      } else {
        cbExists(null, { exists: true, description: data });
      }
    });
  };

  var create = function (r, cbCreate) {
    if (!r.exists) {
      client.createTable(r.definition, function (err, data) {
        if (err) {
          console.error('Error while creating ' + r.definition.TableName + ': ' + JSON.stringify(err, null, 2));
          cbCreate(err);
        } else {
          cbCreate(null, { Table: { TableName: data.TableDescription.TableName, TableStatus: data.TableDescription.TableStatus } });
        }
      });
    } else {
      cbCreate(null, r.description);
    }
  };

  async.parallel([
    function(done) {
      async.compose(create, exists)(params, done);
    },
    function(done){
      waitForTableExists(client, params.TableName, done);
    }
  ], function (err) {
    debug('createTableIfNotExists', JSON.stringify(err, null, 2));
    if (err) callback(err);
    else callback(null);
  });
};

module.exports = DynamoDB;
