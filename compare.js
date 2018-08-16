const { Client } = require('pg')
var fs = require('fs');
const arguments = require('node-command-line-option'); 




class Connection{

	constructor(user = 'postgres', host = 'localhost', database, password = 'postgres', port = 5432, schema){
		this.user = user;
		this.host = host;
		this.database = database;
		this.password = password;
		this.port = port;
		this.schema = schema ? schema : database;
		this.client = new Client({
			user: user,
			host: host,
			database: database,
			password: password,
			port: port 
		});
	}
	connect(){
		console.log(`Connecting to ${this.host}:${this.port}/${this.database}`);
		this.client.connect();
		console.log('Connected');
	}
	close(){
		this.client.end();
	}

	loadTables(){
		console.log('Loading tables from schema '+ this.schema);
		const promise = this.client.query(`SELECT * FROM information_schema.tables WHERE table_schema = '${this.schema}';`);
		return promise.then((result)=>{
			this.rawTables = result.rows;
		});
	}

	async loadTableColumns(){
		this.tables = [];
		for(const rawTable of this.rawTables){
			const table = new Table(this, rawTable);
			this.tables.push(table);
			await table.loadColumns();
		}
		this.rawTables = null;
	}


	writeOutput(){
		var stream = fs.createWriteStream(this.database+'.txt', {
		 // flags: 'a' // 'a' means appending (old data will be preserved)
		});

		this.tables.sort((a,b)=>{
			return a.rawTable.table_name.localeCompare(b.rawTable.table_name);
		});

		for(const table of this.tables){
			stream.write(table.rawTable.table_name);
			stream.write('\n');
			for(const column of table.columns){
				stream.write('	');
				stream.write(column.column_name + ' ' + column.data_type);
				stream.write('\n');
			}
		}
		stream.end();
	}


	getTableByName(name){
		for(const table of this.tables){
			if(table.rawTable.table_name == name){
				return table;
			}
		}
		return null;
	}

	


}


class Table{

	constructor(connection, rawTable){
		this.connection = connection;
		this.rawTable = rawTable;
	}

	loadColumns(){
		const promise = this.connection.client.query(`SELECT * FROM information_schema.columns WHERE table_schema = '${this.connection.schema}' AND table_name   = '${this.rawTable.table_name}'`);
		return promise.then((result)=>{
			this.columns = result.rows;
		});
	}


}



async function compare(options){

	const client1 = new Connection(options.user1, options.host1, options.database1, options.password1, options.port1);
	await client1.connect();

	const client2 = new Connection(options.user2, options.host2, options.database2, options.password2, options.port2);
	await client2.connect();


	await client1.loadTables();
	await client2.loadTables();

	await client1.loadTableColumns();
	await client2.loadTableColumns();


	//client1.writeOutput();
	//client2.writeOutput();


	
	const newTables = [];//tables that are new in client1
	const equalTables = [];//tables that have the same name but different columns
	const sameTables = [];//tables that are 100% the same
	const missingTables = [];//tables that are new in client2

	for(const table of client1.tables){
		const otherTable = client2.getTableByName(table.rawTable.table_name);
		if(otherTable == null){
			newTables.push(table);
		}else{
			let isSame = otherTable.columns.length == table.columns.length;
			if(isSame){
				sameTables.push(table);
			}else{
				equalTables.push(table);
			}
		}
	}

	for(const table of client2.tables){
		const otherTable = client1.getTableByName(table.rawTable.table_name);
		if(otherTable == null){
			missingTables.push(table);
		}
	}
	
	console.log("New Tables");
	for(const table of newTables){
		console.log(table.rawTable.table_name);
	}

	console.log("Missing Tables");
	for(const table of missingTables){
		console.log(table.rawTable.table_name);
	}

	console.log("Equal but not same Tables");
	for(const table of equalTables){
		console.log(table.rawTable.table_name);
	}



	client1.close();
	client2.close();
}
compare(arguments.getOptions());

