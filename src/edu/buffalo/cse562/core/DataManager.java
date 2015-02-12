package edu.buffalo.cse562.core;

import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;

public class DataManager {

	private static DataManager instance;
	private Map<String, Schema> schemas = new HashMap<String, Schema>();
	private Map<String, Table> tables = new HashMap<String, Table>();

	public static DataManager getInstance() {
		if (instance == null)
			instance = new DataManager();
		return instance;
	}

	public void addNewTable(Schema schema, Table table) {
		schemas.put(table.getName(), schema);
		tables.put(table.getName(), table);
	}

	public Schema getSchema(String tableName) {
		return schemas.get(tableName);
	}

	public Table getTable(String tableName) {
		return tables.get(tableName);
	}

}