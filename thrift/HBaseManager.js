var Scan = require("./scan");
//var get = require("./get");
var Put = require("./put");

var HBaseManager = {
	/* request information */
	requestInfo:function (model, id) {
		return 'The purchase info for ' + model + ' with ID ' + id + ' is being processed...';
	},
	/* purchase the car */
	buyVehicle:function (model, id) {
		return 'You have successfully purchased Item ' + id + ', a ' + model + '.';
	}
};


HBaseManager.fire = function (commad) {
	return HBaseManager[commad.request](commad.model, commad.carID);
};


var HBaseAction = {

	scan:function(model, process){
		var scan = new Scan().action(model, process.argv[4], process.argv[5], process.argv[6]);	
		return 'scan function';
	},
	get:function(model, rowid){
		return 'get function';
	},
	put:function(model, process){
		var put = new Put().action(model, process.argv[4], process.argv[5], process.argv[6], process.argv[7]);	
		return 'put function';
	}
};

HBaseManager.execute = function(commad) {	
	return HBaseAction[commad.request](commad.model, commad.data);
};


module.exports = HBaseManager;




