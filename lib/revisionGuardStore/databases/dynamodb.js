'use strict';

var util = require('util'),
  Store = require('../base'),
  _ = require('lodash'),
  uuid = require('uuid/v1'),
  async = require('async'),
  aws = Store.use('aws-sdk');

function DynamoDB(options) {
  Store.call(this, options);

  var defaults = {
    revisionTableName: 'revision',
    ttl:  1000 * 60 * 60 * 1, // 1 hour
    endpointConf: {
      endpoint: 'http://localhost:4567' //dynalite
    },
    EventsReadCapacityUnits: 1,
    EventsWriteCapacityUnits: 3,
  };
  if (process.env['AWS_DYNAMODB_ENDPOINT']) {
    defaults.endpointConf = process.env['AWS_DYNAMODB_ENDPOINT'];
  }
  _.defaults(options, defaults);
  this.options = options;
}

util.inherits(DynamoDB, Store);

_.extend(DynamoDB.prototype, {

  connect: function (callback) {
    var self = this;
    self.client = new aws.DynamoDB(self.options.endpointConf);
    self.documentClient = new aws.DynamoDB.DocumentClient(self.client);
    self.isConnected = true;
    function revisionTableDefinition(opts) {
      var def = {
        TableName: opts.revisionTableName,
        KeySchema: [
          { AttributeName: 'id', KeyType: 'HASH' },
          { AttributeName: 'revision', KeyType: 'RANGE' }
        ],
        AttributeDefinitions: [
          { AttributeName: 'id', AttributeType: 'S' },
          { AttributeName: 'revision', AttributeType: 'S' }
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
        error('connect error: ' + err);
        if (callback) callback(err);
      } else {
        self.emit('connect');
        if (callback) callback(null, self);
      }
    });
  },

  disconnect: function (callback) {
    this.emit('disconnect');
    if (callback) callback(null);
  },

  getNewId: function(callback) {
    callback(null, uuid());
  },

  /// FIXME
  get: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      return callback(err);
    }

    var params = {
      TableName : this.options.revisionTableName,
      KeyConditionExpression: '#id = :id',
      ExpressionAttributeNames:{
          '#id': 'id'
      },
      ExpressionAttributeValues: {
          ':id':id
      }
    };

    this.client.query(params, function (err, data) {
      if (err) {
        return callback(err);
      }

      if (!data|| !data.Item|| !data.Item.revision) {
        return callback(null, null);
      }

      callback(null, data.Item.revision.S || null);
    });
  },

  /// FIXME
  set: function (id, revision, oldRevision, callback) {
    if (!id || !_.isString(id)) {
      return callback(new Error('Please pass a valid id!'));
    }
    if (!revision || !_.isNumber(revision)) {
      var err = new Error('Please pass a valid revision!');
      return callback(err);
    }

    var params = {
     Item: {
       id: {S: id},
       revision: {S: revision}
     },
     TableName: this.options.revisionTableName,
     ReturnConsumedCapacity: 'TOTAL',
    };
    this.client.putItem(params, function (err, data) {
      if (callback) {
        callback(err);
      }
    });
  },

  /// FIXME
  saveLastEvent: function (evt, callback) {
    // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#putItem-property
    var params = {
     Item: {
       id: {S: 'THE_LAST_SEEN_EVENT'},
       revision: {S: JSON.stringify(evt)}
     },
     TableName: this.options.revisionTableName,
     ReturnConsumedCapacity: 'TOTAL',
    };
    this.client.putItem(params, function (err) {
      if (callback) { callback(err); }
    });
  },

  /// FIXME
  getLastEvent: function (callback) {
    // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#getItem-property
    var params = {
     Key: {id: {S: 'THE_LAST_SEEN_EVENT'}},
     TableName: this.options.revisionTableName
    };
    this.client.getItem(params, function (err, entry) {
      if (err) {
        return callback(err);
      }

      if (!entry) {
        return callback(null, null);
      }

      callback(null, JSON.parse(entry.Item.revision.S) || null);
    });
  },

  clear: function (callback) {
    this.client.deleteTable({ TableName: this.options.revisionTableName}, callback);
  }

});

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

  var active = function (d, cbActive) {
    var status = d.Table.TableStatus;
    async.until(
      function () { return status === 'ACTIVE'; },
      function (cbUntil) {
        client.describeTable({ TableName: d.Table.TableName }, function (err, data) {
          if (err) {
            console.error('There was an error checking ' + d.Table.TableName + ' status: ' + JSON.stringify(err, null, 2));
            cbUntil(err);
          } else {
            status = data.Table.TableStatus;
            setTimeout(cbUntil(null, data), 1000);
          }
        });
      },
      function (err, r) {
        if (err) {
          console.error('connect create table error: ' + err);
          return cbActive(err);
        }
        cbActive(null, r);
      });
  };

  async.compose(active, create, exists)(params, function (err, result) {
    if (err) callback(err);
    else callback(null, result);
  });
};

module.exports = DynamoDB;
