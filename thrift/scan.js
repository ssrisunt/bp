var thrift = require('thrift');
var HBase = require('./gen-nodejs/THBaseService');
var HBaseTypes = require('./gen-nodejs/hbase_types');
var connection;

function Scan() {
  connection = thrift.createConnection('192.168.113.112', 9090, {
    transport: thrift.TFramedTransport,
    protocol: thrift.TBinaryProtocol
  });

};

Scan.prototype.action = function(table, startRow, stopRow, family, qualifier) {

connection.on('connect', function () {
  console.log('connected');
  var client = thrift.createClient(HBase, connection);

  var tScanner = new HBaseTypes.TScan({startRow: startRow, stopRow: stopRow, 
    columns: [new HBaseTypes.TColumn({family: family})]});

  client.openScanner(table, tScanner, function (err, scannerId) {
    if (err) {
      console.log(err);
      return;
    }
    console.log('scannerid : ' + scannerId);
    client.getScannerRows(scannerId, 100000000, function (serr, data) {
      if (serr) {
        console.log(serr);
        return;
      }

      if(data.length > 0) {
          console.log(data.length);
          //var cols = data.columnValues;
          var result = [];
          //console.log(data);
          
          data.forEach(function(rawdata) {
            console.log(rawdata);
            //var base = new Buffer(rawdata.value, 'binary');
  
            //result.push({family: rawdata.family+rawdata.qualifier, value: rawdata.value, v:bb});
          }); 
          

      }      
    });
    client.closeScanner(scannerId, function (err) {
      if (err) {
        console.log(err);
      }
    });
    connection.end();
  });
});

connection.on('error', function(err){
  console.log('error', err);
});

};

module.exports = Scan;