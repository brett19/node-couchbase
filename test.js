var CouchbaseClient = require('./lib/couchbase').Connection;

var tst = new CouchbaseClient({
  ssl: true
});

tst.test('testkeya', {}, function(err, res) {
  console.log('tst.test', err, res);
});

/*
tst.set('testkeya', 'franklyn', {}, function(err, res) {
  console.log('tst.set', err, res);

  tst.get('testkeya', {}, function(err, res) {
    console.log('tst.get', err, res);
  });
});
*/
