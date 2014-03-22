"use strict";

var http = require('http');
var crc32 = require('./crc32');
var util = require('util');

var EventEmitter = require('events').EventEmitter;
var CbMemdClient = require('./cbmemcached');

require('buffer').INSPECT_MAX_BYTES = 100;

/**
 * @struct
 */
function BucketConfig() {
  this.vBucketServerMap = null;
}

/**
 * @struct
 */
function BucketMap() {
  this.hashAlgorithm = null;
  this.serverList = null;
  this.sslServerList = null;
  this.vBucketMap = null;
}

function CouchbaseClient(options) {
  if (!options) {
    options = {};
  }

  this.hosts = options.hosts ? options.hosts : ['localhost:8091'];
  this.bucket = options.bucket ? options.bucket : 'default';
  this.password = options.password ? options.password : '';
  this.ssl = options.ssl ? options.ssl : false;

  this._seqNumber = 0;
  this._bucketMap = null;

  this._serverList = {};
  this._serverLookup = [];

  this._bucketQueue = {};
  this._pending = [];
  this._pendingFor = [];
  this._callbackTable = {};

  this._monitorConfig();
}
util.inherits(CouchbaseClient, EventEmitter);

CouchbaseClient.prototype._pickConfigNode = function() {
  return this.hosts[Math.floor(Math.random()*this.hosts.length)];
};

CouchbaseClient.prototype._mapBucket = function(key) {
  var keyCrc = crc32(key);
  return keyCrc % this._bucketMap.vBucketMap.length;
};

CouchbaseClient.prototype._onNewConfig = function(config) {
  var bucketMap = config.vBucketServerMap;

  if (bucketMap.hashAlgorithm !== 'CRC') {
    throw new Error('Bad Hashing Algorithm');
  }

  var serverList = bucketMap.serverList;
  if (this.ssl) {
    serverList = bucketMap.sslServerList;
  }

  this._serverLookup = [];
  for (var i = 0; i < serverList.length; ++i) {
    var serverName = serverList[i];

    if (!this._serverList[serverName]) {
      var hpSplit = serverName.split(':');
      var host = hpSplit[0];
      var port = parseInt(hpSplit[1], 10);

      var client = new CbMemdClient(host, port, this.ssl, this.bucket, this.password);
      client.on('bucketConnect', function() {
        console.log('Connected to bucket.');
        this._descheduleFor();
      }.bind(this));
      this._serverList[serverName] = client;
    }

    this._serverLookup[i] = this._serverList[serverName];
  }

  this._bucketMap = bucketMap;
  this._deschedule();
  this._descheduleFor();
};

CouchbaseClient.prototype._monitorConfig = function() {
  var self = this;

  console.log('Opening streaming config connection.');

  var configStream = '';
  var hostString = this._pickConfigNode().split(':');

  var hostname = hostString[0];
  var port = parseInt(hostString[1], 10);

  var req = http.request({
    hostname: hostname,
    port: port,
    method: 'GET',
    auth: '', // username:password
    path: '/pools/default/bucketsStreaming/' + this.bucket,
    agent: false
  }, function(res) {
    res.setEncoding('utf8');
    res.on('data', function(chunk) {
      configStream += chunk;

      var configBlockEnd = configStream.indexOf('\n\n\n\n');
      if (configBlockEnd >= 0) {
        var myBlock = configStream.substr(0, configBlockEnd);
        configStream = configStream.substr(configBlockEnd+4);

        myBlock.replace(/$HOST/g, hostname);

        var config = JSON.parse(myBlock);
        self._onNewConfig(config);
      }
    });

    res.on('error', function(e) {
      console.log('Config listener Error', e);
    });

    res.on('close', function() {
      console.log('Lost config listener');

      self._monitorConfig();
    });
  });
  req.on('socket', function(sock) {
    // Don't keep the event loop stuck for this
    //sock.unref();
  });
  req.on('error', function(e) {
    console.log('Request Error', e);
  });
  req.end();
};








CouchbaseClient.prototype._deschedule = function() {
  for (var i = 0; i < this._pending.length; ++i) {
    var opInfo = this._pending[i];
    opInfo[0].call(this, opInfo[1]);
  }
  this._pending = [];
};

CouchbaseClient.prototype._schedule = function(op, options) {
  if (this._bucketMap) {
    op.call(this, options);
  } else {
    this._pending.push([op, options]);
  }
};




CouchbaseClient.prototype._getServer = function(vbId, replicaId) {
  if (vbId < 0 || vbId >= this._bucketMap.vBucketMap.length) {
    throw new Error('invalid bucket id.');
  }
  var repList = this._bucketMap.vBucketMap[vbId];

  if (replicaId < 0 || replicaId >= repList.length) {
    throw new Error('invalid replica id.');
  }
  var serverId = repList[replicaId];

  var server = this._serverLookup[serverId];
  if (!server || !server.connected) {
    return null;
  }
  return server;
};

CouchbaseClient.prototype._tryDispatch = function(op) {
  var server = this._getServer(op.vbId, op.replicaId);
  if (server) {
    // This writes to the wire, beyond this point, the operation is now
    //   uncancellable or we could end up executing the operation multiple
    //   times on the cluster side.
    var seqNo = op.handler.call(server, op.options, op.callback);

    // Store a reference to where the op was dispatched so we can
    //   cancel the callback in the future if needed
    op.server = server;
    op.seqNo = seqNo;

    // Clean up some memory we don't need anymore
    delete op.vbId;
    delete op.replicaId;
    delete op.handler;
    delete op.options;
    delete op.callback;

    return true;
  }

  return false;
};

CouchbaseClient.prototype._scheduleFor = function(vbId, replicaId, timeout, handler, options, callback) {
  var op = {
    vbId: vbId,
    replicaId: replicaId,
    handler: handler,
    options: options,
    callback: function(err, data) {
      if (op.timer) {
        clearTimeout(op.timer);
        op.timer = null;
      }
      callback(err, data);
    },
    server: null,
    seqNo: 0
  };

  if (!this._tryDispatch(op)) {
    this._pendingFor.push(op);
  }

  if (timeout > 0) {
    op.timer = setTimeout(function() {
      // Cancel the pending callbacks elsewhere
      if (!op.server) {
        var pendingIdx = this._pendingFor.indexOf(op);
        if (pendingIdx !== -1) {
          this._pendingFor.splice(pendingIdx, 1);
        }
      } else {
        op.server.cancelOp(op.seqNo);
      }

      // Dispatch timeout error
      callback('timeout', null);
    }.bind(this), timeout);
  }
};

CouchbaseClient.prototype._descheduleFor = function() {
  var newPendingFor = [];
  for (var i = 0; i < this._pendingFor.length; ++i) {
    var op = this._pendingFor[i];

    if (!this._tryDispatch(op)) {
      newPendingFor.push(op);
    }
  }
  this._pendingFor = newPendingFor;
};



CouchbaseClient.prototype._set = function(options) {
  var vbId = this._mapBucket(options.key);

  var value = new Buffer(JSON.stringify(options.value));
  var flags = 0;
  var datatype = 0;

  this._scheduleFor(vbId, 0, 2500, CbMemdClient.prototype.set, {
    key: options.key,
    vbId: vbId,
    cas: options.cas,
    expiry: options.expiry,
    flags: flags,
    datatype: datatype,
    value: value
  }, function(err, data) {
    if (err) {
      options.callback(new Error('error: ' + err), null);
      return;
    }

    if (options.callback) {
      options.callback(null, {
        cas: data.cas
      });
    }
  }.bind(this));
};

CouchbaseClient.prototype._get = function(options) {
  var vbId = this._mapBucket(options.key);

  this._scheduleFor(vbId, 0, 2500, CbMemdClient.prototype.get, {
    key: options.key,
    vbId: vbId
  }, function(err, data) {
    if (err) {
      options.callback(new Error('error: ' + err), null);
      return;
    }

    var value = JSON.parse(data.value.toString('utf8'));

    if (options.callback) {
      options.callback(null, {
        value: value,
        flags: data.flags,
        cas: data.cas
      });
    }

  }.bind(this));
};

CouchbaseClient.prototype._test = function(options) {
  var vbId = this._mapBucket(options.key);

  this._scheduleFor(vbId, 0, 0, CbMemdClient.prototype.uprOpenChannel, {
    name: 'teststream'
  }, function(err, data) {
    if (err) {
      options.callback(new Error('error: ' + err), null);
      return;
    }

    this._scheduleFor(vbId, 0, 0, CbMemdClient.prototype.uprStreamRequest, {
      vbId: vbId
    }, function(err, data) {
      if (err) {
        options.callback(new Error('error: ' + err), null);
        return;
      }

      if (options.callback) {
        options.callback(null, data);
      }
    }.bind(this));

  }.bind(this));
};



function wrapKOC(func) {
  return function(key, options, callback) {
    if (typeof(options) === 'Function') {
      callback = options;
      options = {};
    }
    if (!options) {
      options = {};
    }

    options.key = key;
    options.callback = callback;
    return this._schedule(func, options);
  };
}
function wrapKVOC(func) {
  return function(key, value, options, callback) {
    if (typeof(options) === 'Function') {
      callback = options;
      options = {};
    }
    if (!options) {
      options = {};
    }

    options.key = key;
    options.value = value;
    options.callback = callback;
    return this._schedule(func, options);
  };
}

CouchbaseClient.prototype.set =
  wrapKVOC(CouchbaseClient.prototype._set);
CouchbaseClient.prototype.get =
  wrapKOC(CouchbaseClient.prototype._get);
CouchbaseClient.prototype.test =
  wrapKOC(CouchbaseClient.prototype._test);

module.exports.Connection = CouchbaseClient;
