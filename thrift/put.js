var thrift = require('thrift');
var HBase = require('./gen-nodejs/THBaseService');
var HBaseTypes = require('./gen-nodejs/hbase_types');
var connection;

function Put() {
  connection = thrift.createConnection('192.168.113.112', 9090, {
    transport: thrift.TFramedTransport,
    protocol: thrift.TBinaryProtocol
  });
};

Put.prototype.action = function(table, rowKey, family, qualifier, value) {
  connection.on('connect', function () {
    console.log('connected');
    var client = thrift.createClient(HBase, connection);

    var tPut = new HBaseTypes.TPut({row: rowKey,
      columnValues: [
      //new HBaseTypes.TColumnValue({family: 'cf', qualifier: qualifier, value: value, timestamp: 1423851659315}),
      new HBaseTypes.TColumnValue({family: family, qualifier: qualifier, value: value})
      ]});
    client.put(table, tPut, function (err) {
      if (err) {
        console.log(err);
        return;
      }
      console.log('success');
      connection.end();
    });
  });

  connection.on('error', function(err){
    console.log('error', err);
  });

};

module.exports = Put;