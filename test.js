var x = new Buffer(4);

function fastWrite(buf, off, val, bytes) {
  for (var i = 0; i < bytes; ++i) {
    buf[off+bytes-i-1] = val >> (i * 8);
  }
}

(function(){
  var stime = process.hrtime();

  for (var i = 0; i < 1000000; ++i) {
    x.writeUInt32BE(0xF1F2F3F4, 0);
  }

  var etime = process.hrtime(stime);
  console.log(etime);
})();
console.log(x);

(function(){
  var stime = process.hrtime();

  for (var i = 0; i < 1000000; ++i) {
    fastWrite(x, 0, 0xF1F2F3F4, 4);
  }

  var etime = process.hrtime(stime);
  console.log(etime);
})();
console.log(x);

return;

var CouchbaseClient = require('./lib/couchbase').Connection;

var tst = new CouchbaseClient({});

var randval = 'xxx-' + (Math.random() * 1000000);

function doOne() {
  tst.set('JSCBC-test-set-flags-override10', randval, {}, function(err, res) {
    console.log(err, res);
  });

  tst.get('JSCBC-test-set-flags-override10', {}, function(err, res) {
    console.log(err, res);

    doOne();
  });
}
for (var i = 0; i < 10; ++i) {
  doOne();
}