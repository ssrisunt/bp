var HBaseManager = require("./HBaseManager");
var action = process.argv[2];
var tableName = process.argv[3];

/* scan command:
 * node command.js action tableName startRow endRow cf qualifier
 * e.g., node command.js scan mdays 0001C1FE-DFB9-4136-83D0-68B2F9ADA0E5 0001C1FE-DFB9-4136-83D0-68B2F9ADA0E6 BatchProcessResult BP1
 */  

/* put command:
 * node command.js action tableName rowkey cf qualifier value
 * e.g., node command.js put RDBPHistory 14820933016 cf BP1 14820933016
 */  
 
var actionA = HBaseManager.execute({request:action, model:tableName, data:process});
console.log(actionA);


/* put command:
 * node command.js action tableName rowkey cf qualifier value
 * e.g., node command.js put RDBPHistory 14820933016 cf BP1 14820933016
 */  

//var actionB = HBaseManager.execute({request:'put', model:tableName, data:process});
//console.log(actionB);



