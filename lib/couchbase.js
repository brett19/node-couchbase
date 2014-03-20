"use strict";

var http = require('http');
var net = require('net');
var tls = require('tls');
var util = require('util');
var crc32 = require('./crc32');

var EventEmitter = require('events').EventEmitter;

require('buffer').INSPECT_MAX_BYTES = 100;

function bufferFastWrite(buf, off, val, bytes) {
  for (var i = 0; i < bytes; ++i) {
    buf[off+bytes-i-1] = val >> (i * 8);
  }
}

function bufferWriteCas(buf, off, cas) {
  if (cas && Array.isArray(cas) && cas.length === 2) {
    bufferFastWrite(buf, off+0, cas[0], 4);
    bufferFastWrite(buf, off+1, cas[1], 4);
  } else {
    bufferFastWrite(buf, off+0, 0, 4);
    bufferFastWrite(buf, off+1, 0, 4);
  }
}

function bufferWriteReq(buf, op, dataType, bucketId, seqNo, cas, extLen, keyLen, dataLen) {
  bufferFastWrite(buf, 0, MEMCACHED_REQUEST_MAGIC, 1);
  bufferFastWrite(buf, 1, op, 1);
  bufferFastWrite(buf, 2, keyLen, 2);
  bufferFastWrite(buf, 4, extLen, 1);
  bufferFastWrite(buf, 5, dataType, 1);
  bufferFastWrite(buf, 6, bucketId, 2);
  bufferFastWrite(buf, 8, extLen+keyLen+dataLen, 4);
  bufferFastWrite(buf, 12, seqNo, 4);
  bufferWriteCas(buf, 16, cas);
}

function makePacket(op, dataType, bucketId, seqNo, cas, extLen, key, value) {
  var keyLength = 0;
  if (key) {
    keyLength = Buffer.byteLength(key);
  }

  var valueLength = 0;
  if (value) {
    valueLength = value.length;
  }

  var buf = new Buffer(24 + extLen + keyLength + valueLength);

  bufferWriteReq(buf,
    op,
    dataType,
    bucketId,
    seqNo,
    cas,
    extLen,
    keyLength,
    valueLength);

  if (keyLength > 0) {
    buf.write(key, 24+extLen, keyLength);
  }
  if (valueLength > 0) {
    value.copy(buf, 24+extLen+keyLength);
  }

  return buf;
}

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
  this.serverList = null;
  this.vBucketMap = null;
  this.hashAlgorithm = null;
}
/**
 * @type BucketMap
 */
var bucketMap = null;


function CouchbaseClient(options) {
  if (!options) {
    options = {};
  }

  this.hosts = options.hosts ? options.hosts : ['localhost:8091'];
  this.bucket = options.bucket ? options.bucket : 'default';
  this.password = options.password ? options.password : '';
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

  this._serverLookup = [];
  for (var i = 0; i < bucketMap.serverList.length; ++i) {
    var serverName = bucketMap.serverList[i];

    if (!this._serverList[serverName]) {
      var hpSplit = serverName.split(':');
      var host = hpSplit[0];
      var port = parseInt(hpSplit[1], 10);

      var client = new CbMemdClient(host, port, false, this.bucket, this.password);
      client.on('bucketConnect', function() {
        console.log('MEMD CONNECTED');
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

  var configStream = '';
  var randomNode = this._pickConfigNode().split(':');
  var req = http.request({
    hostname: randomNode[0],
    port: randomNode[1],
    method: 'GET',
    auth: '', // username:password
    path: '/pools/default/bucketsStreaming/' + this.bucket
  }, function(res) {
    res.setEncoding('utf8');
    res.on('data', function(chunk) {
      configStream += chunk;

      var configBlockEnd = configStream.indexOf('\n\n\n\n');
      if (configBlockEnd >= 0) {
        var myBlock = configStream.substr(0, configBlockEnd);
        configStream = configStream.substr(configBlockEnd+4);

        var config = JSON.parse(myBlock);
        self._onNewConfig(config);
      }
    });

    res.on('error', function(e) {
      console.log('Response Error', e);
    });
  });
  req.on('error', function(e) {
    console.log('Request Error', e);
  });
  req.end();
};

var MEMCACHED_REQUEST_MAGIC = 0x80;
var MEMCACHED_RESPONSE_MAGIC = 0x81;

var MEMCACHED_OP = {
  GET: 0x00,
  SET: 0x01,
  ADD: 0x02,
  REPLACE: 0x03,
  DELETE: 0x04,
  INCREMENT: 0x05,
  DECREMENT: 0x06,
  QUIT: 0x07,
  FLUSH: 0x08,
  GETQ: 0x09,
  NOOP: 0x0a,
  VERSION: 0x0b,
  GETK: 0x0c,
  GETKQ: 0x0d,
  APPEND: 0x0e,
  PREPEND: 0x0f,
  STAT: 0x10,
  SETQ: 0x11,
  ADDQ: 0x12,
  REPLACEQ: 0x13,
  DELETEQ: 0x14,
  INCREMENTQ: 0x15,
  DECREMENTQ: 0x16,
  QUITQ: 0x17,
  FLUSHQ: 0x18,
  APPENDQ: 0x19,
  PREPENDQ: 0x1a,
  VERBOSITY: 0x1b,
  TOUCH: 0x1c,
  GAT: 0x1d,
  GATQ: 0x1e,

  SASL_LIST_MECHS: 0x20,
  SASL_AUTH: 0x21,
  SASL_STEP: 0x22,

  UPR_OPEN: 0x50,
  UPR_ADD_STREAM: 0x51,
  UPR_CLOSE_STREAM: 0x52,
  UPR_STREAM_REQ: 0x53,
  UPR_FAILOVER_LOG_REQ: 0x54,
  UPR_SNAPSHOT_MARKER: 0x56,
  UPR_MUTATION: 0x57,
  UPR_DELETION: 0x58,
  UPR_EXPIRATION: 0x59,
  UPR_FLUSH: 0x5a,
  UPR_SET_VBUCKET_STATE: 0x5b
};

function CbMemdClient(host, port, ssl, bucket, password) {
  // SSL Test
  ssl = true;
  port = 11207;

  this.host = host;
  this.port = port;
  this.ssl = ssl;
  this.bucket = bucket;
  this.password = password;

  this.connected = false;
  this.socket = null;
  this.dataBuf = null;
  this.seqNo = 1;
  this.activeOps = {};

  this._tryConnect();
}
util.inherits(CbMemdClient, EventEmitter);

CbMemdClient.prototype._tryConnect = function() {
  if (!this.ssl) {
    this.socket = net.createConnection({
      host: this.host,
      port: this.port
    }, this._onConnect.bind(this));
  } else {
    this.socket = tls.connect({
      host: this.host,
      port: this.port,
      rejectUnauthorized: false
    }, this._onConnect.bind(this));
  }

  this.socket.setNoDelay(true);
  this.socket.on('error', this._onError.bind(this));
  this.socket.on('data', this._handleData.bind(this));
};

CbMemdClient.prototype._onConnect = function() {
  this._saslAuthPlain({}, function(err, data) {
    if (err) {
      throw new Error('failed to authenticate');
    }

    this.connected = true;
    this.emit('bucketConnect');
  }.bind(this));
};

CbMemdClient.prototype._onError = function(err) {
  console.log('CbMemdClient::error', err);
};

CbMemdClient.prototype._handlePacket = function(data) {
  var magic = data.readUInt8(0);
  if (magic !== MEMCACHED_RESPONSE_MAGIC) {
    throw new Error('not a response');
  }

  // Early out!
  var seqNo = data.readUInt32BE(12);
  var opInfo = this.activeOps[seqNo];
  if (!opInfo) {
    return;
  }
  delete this.activeOps[seqNo];

  var opCode = data.readUInt8(1);
  var keyLen = data.readUInt16BE(2);
  var extLen = data.readUInt8(4);
  var datatype = data.readUInt8(5);
  var statusCode = data.readUInt16BE(6);
  var value = null;
  if (data.length > 24+extLen+keyLen) {
    value = data.slice(24+extLen+keyLen);
  }

  if (statusCode !== 0) {
    opInfo.callback(statusCode, {
      key: opInfo.key,
      info: value.toString()
    });
    return;
  }

  var cas = [
    data.readUInt32BE(16),
    data.readUInt32BE(20)
  ];
  //var key = null;
  //if (keyLen > 0) {
  //  key = data.slice(24+extLen, 24+extLen+keyLen);
  //}

  if (opCode === MEMCACHED_OP.GET) {
    var flags = 0;

    if (extLen >= 4) {
      flags = data.readUInt32BE(24);
    }

    opInfo.callback(null, {
      key: opInfo.key,
      datatype: datatype,
      flags: flags,
      cas: cas,
      value: value
    });
  } else if (opCode === MEMCACHED_OP.SET) {
    opInfo.callback(null, {
      key: opInfo.key,
      cas: cas
    });
  } else if (opCode === MEMCACHED_OP.SASL_AUTH) {
    opInfo.callback(null, {
      message: value.toString()
    });
  } else {
    console.log('unknown response packet');
    console.log(data, value.toString());
  }
};

CbMemdClient.prototype._tryReadPacket = function(data, off) {
  if (data.length >= off+24) {
    var bodyLen = data.readUInt32BE(off+8);
    var packetLen = 24 + bodyLen;
    if (data.length >= off+packetLen) {
      this._handlePacket(data.slice(off, off+packetLen));
      return packetLen;
    } else {
      return 0;
    }
  } else {
    return 0;
  }
};

CbMemdClient.prototype._handleData = function(data) {
  if (this.dataBuf === null) {
    this.dataBuf = data;
  } else {
    var totalLen = this.dataBuf.length + data.length;
    this.dataBuf = Buffer.concat([this.dataBuf, data], totalLen);
  }

  var offset = 0;
  while (offset < this.dataBuf.length) {
    var packetLen = this._tryReadPacket(this.dataBuf, offset);
    if (packetLen <= 0) {
      break;
    }

    offset += packetLen;
  }

  if (offset === this.dataBuf.length) {
    this.dataBuf = null;
  } else {
    this.dataBuf = this.dataBuf.slice(offset);
  }
};




CbMemdClient.prototype._saslAuthPlain = function(options, callback) {
  var seqNo = this.seqNo++;

  if (callback) {
    this.activeOps[seqNo] = {
      callback: callback
    };
  }

  var authzid = new Buffer(0);
  var authcid = new Buffer(this.bucket, 'utf8');
  var passwd = new Buffer(this.password, 'utf8');

  var authMech = 'PLAIN';
  var authData = Buffer.concat([
    authzid,
    new Buffer([0]),
    authcid,
    new Buffer([0]),
    passwd
  ]);

  var buf = makePacket(
    MEMCACHED_OP.SASL_AUTH,
    0,
    0,
    seqNo,
    null,
    0,
    authMech,
    authData
  );
  this.socket.write(buf);
};

CbMemdClient.prototype.get = function(options, callback) {
  var seqNo = this.seqNo++;

  if (callback) {
    this.activeOps[seqNo] = {
      key: options.key,
      callback: callback
    };
  }

  var buf = makePacket(
    MEMCACHED_OP.GET,
    0,
    options.vbId,
    seqNo,
    null,
    0,
    options.key,
    null
  );
  this.socket.write(buf);
};

CbMemdClient.prototype.set = function(options, callback) {
  var seqNo = this.seqNo++;

  if (callback) {
    this.activeOps[seqNo] = {
      key: options.key,
      callback: callback
    };
  }

  var buf = makePacket(
    MEMCACHED_OP.SET,
    options.datatype,
    options.vbId,
    seqNo,
    options.cas,
    8,
    options.key,
    options.value
  );
  bufferFastWrite(buf, 24, options.flags, 4);
  bufferFastWrite(buf, 28, options.expiry, 4);
  this.socket.write(buf);
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

CouchbaseClient.prototype._scheduleFor = function(vbId, replicaId, handler, options, callback) {
  var server = this._getServer(vbId, replicaId);
  if (server) {
    handler.call(server, options, callback);
  } else {
    this._pendingFor.push([vbId, replicaId, handler, options, callback]);
  }
};

CouchbaseClient.prototype._descheduleFor = function() {
  var newPendingFor = [];
  for (var i = 0; i < this._pendingFor.length; ++i) {
    var op = this._pendingFor[i];
    var server = this._getServer(op[0], op[1]);
    if (server) {
      op[2].call(server, op[3], op[4]);
    } else {
      newPendingFor.push(op);
    }
  }
  this._pendingFor = newPendingFor;
};

CouchbaseClient.prototype._get = function(options) {
  var vbId = this._mapBucket(options.key);

  this._scheduleFor(vbId, 0, CbMemdClient.prototype.get, {
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

CouchbaseClient.prototype._set = function(options) {
  var vbId = this._mapBucket(options.key);

  var value = new Buffer(JSON.stringify(options.value));
  var flags = 0;
  var datatype = 0;

  this._scheduleFor(vbId, 0, CbMemdClient.prototype.set, {
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









CouchbaseClient.prototype.set = function(key, value, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }

  options.key = key;
  options.value = value;
  options.callback = callback;
  return this._schedule(this._set, options);
};

CouchbaseClient.prototype.get = function(key, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }

  options.key = key;
  options.callback = callback;
  return this._schedule(this._get, options);
};

CouchbaseClient.prototype.getReplica = function(key, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }

  options.key = key;
  options.callback = callback;
  return this._schedule(this._getReplica, options);
};

module.exports.Connection = CouchbaseClient;

/*
var onNewConfiguration = function(config) {
  bucketMap = config.vBucketServerMap;

  if (bucketMap.hashAlgorithm !== 'CRC') {
    throw new Error('Bad Hashing Algorithm');
  }

  var bucketId = mapBucket(testKey);
  var serverId = bucketMap.vBucketMap[bucketId][0];
  var serverHost = bucketMap.serverList[serverId];

  var cl = new MemcachedClient(serverHost);
  cl.connect();
  cl.on('connect', function() {
    console.log('connected');

    var testPak = new MemcachedPacket();
    testPak.magic = 0x80;
    testPak.opCode = 0x00;
    testPak.vBucketId = bucketId;
    testPak.key = testKey;
    cl.writePacket(testPak, function(pak) {
      console.log('got data');
      console.log(pak);
    });

  });
};

var configNodes = ['localhost:8091'];
function pickRandomNode() {
  return configNodes[Math.floor(Math.random()*configNodes.length)];
}

var bucketName = 'default';
var configMonitor = function() {
  var configStream = '';
  var randomNode = pickRandomNode().split(':');
  var req = http.request({
    hostname: randomNode[0],
    port: randomNode[1],
    method: 'GET',
    auth: '', // username:password
    path: '/pools/default/bucketsStreaming/' + bucketName
  }, function(res) {
    res.setEncoding('utf8');
    res.on('data', function(chunk) {
      configStream += chunk;

      var configBlockEnd = configStream.indexOf('\n\n\n\n');
      if (configBlockEnd >= 0) {
        var myBlock = configStream.substr(0, configBlockEnd);
        configStream = configStream.substr(configBlockEnd+4);

        var config = JSON.parse(myBlock);
        onNewConfiguration(config);
      }
    });

    res.on('error', function(e) {
      console.log('Response Error', e);

    });
  });
  req.on('error', function(e) {
    console.log('Request Error', e);
  });
  req.end();
};
configMonitor();
*/