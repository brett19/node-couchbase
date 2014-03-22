var net = require('net');
var tls = require('tls');
var util = require('util');

var EventEmitter = require('events').EventEmitter;

function bufferFastWrite(buf, off, val, bytes) {
  for (var i = 0; i < bytes; ++i) {
    buf[off+bytes-i-1] = val >> (i * 8);
  }
}

function memdPacket(op, dataType, bucketId, seqNo, cas, extLen, key, value) {
  var keyLength = 0;
  if (key) {
    keyLength = Buffer.byteLength(key);
  }

  var valueLength = 0;
  if (value) {
    valueLength = value.length;
  }

  var buf = new Buffer(24 + extLen + keyLength + valueLength);

  bufferFastWrite(buf, 0, MEMCACHED_REQUEST_MAGIC, 1);
  bufferFastWrite(buf, 1, op, 1);
  bufferFastWrite(buf, 2, keyLength, 2);
  bufferFastWrite(buf, 4, extLen, 1);
  bufferFastWrite(buf, 5, dataType, 1);
  bufferFastWrite(buf, 6, bucketId, 2);
  bufferFastWrite(buf, 8, extLen+keyLength+valueLength, 4);
  bufferFastWrite(buf, 12, seqNo, 4);

  if (cas && Array.isArray(cas) && cas.length === 2) {
    bufferFastWrite(buf, 16, cas[0], 4);
    bufferFastWrite(buf, 20, cas[1], 4);
  } else {
    bufferFastWrite(buf, 16, 0, 4);
    bufferFastWrite(buf, 20, 0, 4);
  }

  if (keyLength > 0) {
    buf.write(key, 24+extLen, keyLength);
  }
  if (valueLength > 0) {
    value.copy(buf, 24+extLen+keyLength);
  }

  return buf;
}

var MEMCACHED_REQUEST_MAGIC = 0x80;
var MEMCACHED_RESPONSE_MAGIC = 0x81;

var MEMCACHED_CMD = {
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

  GET_REPLICA: 0x83,

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
  console.log('Creating new memcached connection');

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

CbMemdClient.prototype._handleReqPacket = function(data) {
  // Early out!
  var seqNo = data.readUInt32BE(12);
  var opInfo = this.activeOps[seqNo];
  if (!opInfo) {
    return;
  }
  if (!opInfo.persist) {
    delete this.activeOps[seqNo];
  }

  // Read the header
  var opCode = data.readUInt8(1);
  var keyLen = data.readUInt16BE(2);
  var extLen = data.readUInt8(4);
  var datatype = data.readUInt8(5);
  var vbid = data.readUInt16BE(6);
  var key = null;
  if (keyLen > 0) {
    key = data.slice(24+extLen, 24+extLen+keyLen);
  }
  var value = null;
  if (data.length > 24+extLen+keyLen) {
    value = data.slice(24+extLen+keyLen);
  }

  var cas = [
    data.readUInt32BE(16),
    data.readUInt32BE(20)
  ];

  if (opCode === MEMCACHED_CMD.UPR_SNAPSHOT_MARKER) {
    opInfo.callback(null, {
      op: opCode
    });
  } else if (opCode === MEMCACHED_CMD.UPR_MUTATION) {
    if (extLen < 30) {
      throw new Error('invalid UPR Mutation packet');
    }

    var flags = data.readUInt32BE(40);
    var expire = data.readUInt32BE(44);
    var locktime = data.readUInt32BE(48);

    opInfo.callback(null, {
      op: opCode,
      key: key.toString(),
      value: value,
      flags: flags,
      datatype: datatype,
      expire: expire,
      locktime: locktime
    });
  } else {
    console.log('unknown response packet');
    console.log(data);
  }
};

CbMemdClient.prototype._handleRespPacket = function(data) {
  // Early out!
  var seqNo = data.readUInt32BE(12);
  var opInfo = this.activeOps[seqNo];
  if (!opInfo) {
    return;
  }
  if (!opInfo.persist) {
    delete this.activeOps[seqNo];
  }

  // Read the header
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
    console.log('status code error', statusCode.toString(16), opCode.toString(16));
    opInfo.callback(statusCode, {
      op: opCode,
      key: opInfo.key
    });
    return;
  }

  var cas = [
    data.readUInt32BE(16),
    data.readUInt32BE(20)
  ];

  if (opCode === MEMCACHED_CMD.GET) {
    var flags = 0;

    if (extLen >= 4) {
      flags = data.readUInt32BE(24);
    }

    opInfo.callback(null, {
      op: opCode,
      key: opInfo.key,
      datatype: datatype,
      flags: flags,
      cas: cas,
      value: value
    });
  } else if (opCode === MEMCACHED_CMD.SET) {
    opInfo.callback(null, {
      op: opCode,
      key: opInfo.key,
      cas: cas
    });
  } else if (opCode === MEMCACHED_CMD.SASL_AUTH) {
    opInfo.callback(null, {
      op: opCode,
      message: value.toString()
    });
  } else if (opCode === MEMCACHED_CMD.UPR_OPEN) {
    opInfo.callback(null, {
      op: opCode
    });
  } else if (opCode === MEMCACHED_CMD.UPR_STREAM_REQ) {
    opInfo.callback(null, {
      op: opCode
    });
  } else {
    console.log('unknown response packet');
    console.log(data);
  }
};

CbMemdClient.prototype._handlePacket = function(data) {
  var magic = data.readUInt8(0);
  if (magic === MEMCACHED_RESPONSE_MAGIC) {
    this._handleRespPacket(data);
  } else if (magic === MEMCACHED_REQUEST_MAGIC) {
    this._handleReqPacket(data);
  } else {
    throw new Erro('invalid packet header');
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

CbMemdClient.prototype.cancelOp = function(seqNo) {
  delete this.activeOps[seqNo];
};


CbMemdClient.prototype._saslAuthPlain = function(options, callback) {
  var seqNo = this.seqNo++;

  if (callback) {
    this.activeOps[seqNo] = {
      callback: callback
    };
  }

  var authMech = 'PLAIN';
  var authData = Buffer.concat([
    /*authzid*/
    new Buffer([0]),
    /*authcid*/ new Buffer(this.bucket, 'utf8'),
    new Buffer([0]),
    /*passwd*/ new Buffer(this.password, 'utf8')
  ]);

  var buf = memdPacket(
    MEMCACHED_CMD.SASL_AUTH,
    0,
    0,
    seqNo,
    null,
    0,
    authMech,
    authData
  );
  this.socket.write(buf);

  return seqNo;
};

CbMemdClient.prototype.get = function(options, callback) {
  var seqNo = this.seqNo++;

  if (callback) {
    this.activeOps[seqNo] = {
      key: options.key,
      callback: callback
    };
  }

  var buf = memdPacket(
    MEMCACHED_CMD.GET,
    0,
    options.vbId,
    seqNo,
    null,
    0,
    options.key,
    null
  );
  this.socket.write(buf);

  return seqNo;
};

CbMemdClient.prototype.set = function(options, callback) {
  var seqNo = this.seqNo++;

  if (callback) {
    this.activeOps[seqNo] = {
      key: options.key,
      callback: callback
    };
  }

  var buf = memdPacket(
    MEMCACHED_CMD.SET,
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

  return seqNo;
};

CbMemdClient.prototype.uprOpenChannel = function(options, callback) {
  var seqNo = this.seqNo++;

  if (callback) {
    this.activeOps[seqNo] = {
      callback: callback
    };
  }

  var buf = memdPacket(
    MEMCACHED_CMD.UPR_OPEN,
    0,
    0,
    seqNo,
    null,
    8,
    options.name,
    null
  );
  bufferFastWrite(buf, 24, 0, 4); // Sequence Number
  bufferFastWrite(buf, 28, 1, 4); // Flags (Producer)
  this.socket.write(buf);

  return seqNo;
};

CbMemdClient.prototype.uprStreamRequest = function(options, callback) {
  var seqNo = this.seqNo++;

  if (callback) {
    this.activeOps[seqNo] = {
      persist: true,
      callback: callback
    };
  }

  var buf = memdPacket(
    MEMCACHED_CMD.UPR_STREAM_REQ,
    0,
    options.vbId,
    seqNo,
    null,
    0x28,
    null,
    null
  );
  bufferFastWrite(buf, 24, 0, 4); // Flags
  bufferFastWrite(buf, 28, 0, 4); // Reserved
  bufferFastWrite(buf, 32, 0x00000000, 4); // Start SeqNo (Low)
  bufferFastWrite(buf, 36, 0x00000000, 4); // Start SeqNo (High)
  bufferFastWrite(buf, 40, 0xFFFFFFFF, 4); // End SeqNo (Low)
  bufferFastWrite(buf, 44, 0xFFFFFFFF, 4); // End SeqNo (High)
  bufferFastWrite(buf, 48, 0x00000000, 4); // VBucket UUID (Low)
  bufferFastWrite(buf, 52, 0x00000000, 4); // VBucket UUID (High)
  bufferFastWrite(buf, 56, 0x00000000, 4); // High SeqNo (Low)
  bufferFastWrite(buf, 60, 0x00000000, 4); // High SeqNo (High)
  this.socket.write(buf);

  return seqNo;
};

module.exports = CbMemdClient;