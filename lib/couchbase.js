"use strict";

var http = require('http');
var net = require('net');
var util = require('util');
var crc32 = require('./crc32');

var EventEmitter = require('events').EventEmitter;

require('buffer').INSPECT_MAX_BYTES = 100;

function bufferFastWrite(buf, off, val, bytes) {
  for (var i = 0; i < bytes; ++i) {
    buf[off+bytes-i-1] = val >> (i * 8);
  }
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

/**
 * @constructor
 */
function MemcachedPacket() {
  this.magic = 0x80;
  this.opCode = 0x00;
  this.dataType = 0;
  this.vBucketId = 0;
  this.extras = null;
  this.cas = null;
  this.key = null;
}

function CouchbaseClient(options) {
  if (!options) {
    options = {};
  }

  this.hosts = options.hosts ? options.hosts : ['localhost:8091'];
  this.bucket = options.bucket ? options.bucket : 'default';

  this._seqNumber = 0;
  this._bucketMap = null;

  this._serverList = {};
  this._serverLookup = [];

  this._bucketQueue = {};
  this._pending = [];
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

  this.serverLookup = [];
  for (var i = 0; i < bucketMap.serverList.length; ++i) {
    var serverName = bucketMap.serverList[i];

    if (!this._serverList[serverName]) {
      this._serverList[serverName] = new MemdClient(this, serverName);
    }

    this._serverLookup[i] = this._serverList[serverName];
  }

  this._bucketMap = bucketMap;
  this._deschedule();
};

var MEMCACHED_REQUEST_MAGIC = 0x80;
var MEMCACHED_RESPONSE_MAGIC = 0x81;

var MEMCACHED_OP_GET = 0;
var MEMCACHED_OP_SET = 1;

function MemdClient(parent, name) {
  var nameinfo = name.split(':');
  this.host = nameinfo[0];
  this.port = parseInt(nameinfo[1]);

  this.dataBuf = null;

  this.parent = parent;
  this.connected = false;
  this.connecting = false;
  this.socket = null;
  this.pending = [];

  this.activeOps = {};

  net.Socket.call(this, {});
}
util.inherits(MemdClient, net.Socket);

MemdClient.prototype._tryConnect = function() {
  if (!this.connecting) {
    //this.setNoDelay(true);
    this.setNoDelay(true);
    this.connect(this.port, this.host);
    this.on('connect', this._onConnect);
    this.on('data', this._handleData);
    this.connecting = true;
  }
};

MemdClient.prototype._onConnect = function() {
  this.connected = true;
  this._deschedule();
};

MemdClient.prototype._handlePacket = function(data, off) {
  var magic = data.readUInt8(off+0);
  if (magic !== MEMCACHED_RESPONSE_MAGIC) {
    throw new Exception('not a response');
  }

  // Early out!
  var seqNo = data.readUInt32BE(off+12);
  var opInfo = this.activeOps[seqNo];
  if (!opInfo) {
    return;
  }

  delete this.activeOps[seqNo];
  if (!opInfo.callback) {
    return;
  }

  var opCode = data.readUInt8(off+1);
  var keyLength = data.readUInt16BE(off+2);
  var extrasLength = data.readUInt8(off+4);
  var dataType = data.readUInt8(off+5);
  var statusCode = data.readUInt16BE(off+6);
  var cas = [
    data.readUInt32BE(off+16),
    data.readUInt32BE(off+20)
  ];

  if (opCode === MEMCACHED_OP_GET) {
    if (extrasLength != 4) {
      throw new Exception('missing extras');
    }

    var flags = data.readUInt32BE(off+24);
    //var key = data.toString('utf8', off+24+extrasLength, keyLength);
    var value = data.toString('utf8', off+24+extrasLength+keyLength);

    opInfo.callback(null, {
      value: value,
      flags: flags,
      cas: cas
    });
  } else if (opCode === MEMCACHED_OP_SET) {
    if (statusCode === 0) {
      opInfo.callback(null, {
        cas: cas
      });
    } else {
      opInfo.callback(new Error('set failed : ' + statusCode));
    }
  } else {
    console.log('unknown response packet');
    console.log(data);
  }
};

MemdClient.prototype._tryReadPacket = function(data, off) {
  if (data.length >= 24) {
    var bodyLen = data.readUInt32BE(off+8);
    var packetLen = 24 + bodyLen;
    if (data.length >= packetLen) {
      this._handlePacket(data, off);
      return packetLen;
    } else {
      return 0;
    }
  } else {
    return 0;
  }
};

MemdClient.prototype._handleData = function(data) {
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

MemdClient.prototype._writePacketNow = function(packet) {
  this.activeOps[packet.seqNo] = packet;
  this.write(packet.buf, 'buffer');
};

MemdClient.prototype._deschedule = function() {
  for (var i = 0; i < this.pending.length; ++i) {
    this._writePacketNow(this.pending[i]);
  }
  this.pending = [];
};

MemdClient.prototype._writeBuffer = function(packet) {
  if (this.connected) {
    this._writePacketNow(packet);
  } else {
    this.pending.push(packet);
    this._tryConnect();
  }
};

CouchbaseClient.prototype._writeBuffer = function(packet) {
  var serverId = this._bucketMap.vBucketMap[packet.vbId][0];
  this._serverLookup[serverId]._writeBuffer(packet);
};

CouchbaseClient.prototype._get = function(options, key) {
  var seqNo = this._seqNumber++;

  var dataType = 0;

  var keyLength = Buffer.byteLength(key);
  var buf = new Buffer(24 + keyLength);

  var bucketId = this._mapBucket(key);

  /*
  buf.writeUInt8(MEMCACHED_REQUEST_MAGIC, 0);
  buf.writeUInt8(MEMCACHED_OP_GET, 1);
  buf.writeUInt16BE(keyLength, 2);
  buf.writeUInt8(0, 4);
  buf.writeUInt8(dataType, 5);
  buf.writeUInt16BE(bucketId, 6);
  buf.writeUInt32BE(keyLength, 8);
  buf.writeUInt32BE(seqNo, 12);
  buf.writeUInt32BE(0, 16);
  buf.writeUInt32BE(0, 20);
  */

  bufferFastWrite(buf, 0, MEMCACHED_REQUEST_MAGIC, 1);
  bufferFastWrite(buf, 1, MEMCACHED_OP_GET, 1);
  bufferFastWrite(buf, 2, keyLength, 2);
  bufferFastWrite(buf, 4, 0, 1);
  bufferFastWrite(buf, 5, dataType, 1);
  bufferFastWrite(buf, 6, bucketId, 2);
  bufferFastWrite(buf, 8, keyLength, 4);
  bufferFastWrite(buf, 12, seqNo, 4);
  bufferFastWrite(buf, 16, 0, 4);
  bufferFastWrite(buf, 20, 0, 4);

  buf.write(key, 24, keyLength);

  var packet = {
    seqNo: seqNo,
    vbId: bucketId,
    buf: buf,
    callback: options.callback
  };
  this._writeBuffer(packet);
};

CouchbaseClient.prototype._set = function(options, key) {
  var seqNo = this._seqNumber++;

  var dataType = 0;

  var keyLength = Buffer.byteLength(key);
  var valueLength = Buffer.byteLength(options.value);
  var buf = new Buffer(24 + 8 + keyLength + valueLength);

  var bucketId = this._mapBucket(key);

  /*
   buf.writeUInt8(MEMCACHED_REQUEST_MAGIC, 0);   // magic
   buf.writeUInt8(MEMCACHED_OP_SET, 1);          // opCode
   buf.writeUInt16BE(keyLength, 2);              // keyLength
   buf.writeUInt8(8, 4);                         // extrasLength
   buf.writeUInt8(dataType, 5);                  // dataType
   buf.writeUInt16BE(bucketId, 6);               // vbucketId
   buf.writeUInt32BE(keyLength+8+valueLength, 8);// bodyLength
   buf.writeUInt32BE(seqNo, 12);                 // opaque
   buf.writeUInt32BE(0, 16);                     // CAS[0]
   buf.writeUInt32BE(0, 20);                     // CAS[1]
   buf.writeUInt32BE(0, 24);                     // flags
   buf.writeUInt32BE(0, 28);                     // expiry
   */

  bufferFastWrite(buf, 0, MEMCACHED_REQUEST_MAGIC, 1);
  bufferFastWrite(buf, 1, MEMCACHED_OP_SET, 1);
  bufferFastWrite(buf, 2, keyLength, 2);
  bufferFastWrite(buf, 4, 8, 1);
  bufferFastWrite(buf, 5, dataType, 1);
  bufferFastWrite(buf, 6, bucketId, 2);
  bufferFastWrite(buf, 8, keyLength+8+valueLength, 4);
  bufferFastWrite(buf, 12, seqNo, 4);
  bufferFastWrite(buf, 16, 0, 4);
  bufferFastWrite(buf, 20, 0, 4);
  bufferFastWrite(buf, 24, 0, 4);
  bufferFastWrite(buf, 28, 0, 4);

  buf.write(key, 24+8, keyLength);
  buf.write(options.value, 24+8+keyLength, valueLength);

  var packet = {
    seqNo: seqNo,
    vbId: bucketId,
    buf: buf,
    callback: options.callback
  };
  this._writeBuffer(packet);
};

CouchbaseClient.prototype._deschedule = function() {
  for (var i = 0; i < this._pending.length; ++i) {
    var opInfo = this._pending[i];
    opInfo[0].call(this, opInfo[1], opInfo[2]);
  }
  this._pending = [];
};

CouchbaseClient.prototype._schedule = function(op, options, keys) {
  if (this._bucketMap) {
    op.call(this, options, keys);
  } else {
    this._pending.push([op, options, keys]);
  }
};

CouchbaseClient.prototype.set = function(key, value, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }

  options.value = value;
  options.callback = callback;
  return this._schedule(this._set, options, key);
};

CouchbaseClient.prototype.get = function(key, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }

  options.callback = callback;
  return this._schedule(this._get, options, key);
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