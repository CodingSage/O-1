package edu.buffalo.cse562.core;

import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;

public class DataManager {

	private static DataManager instance;
	// private Map<String, Schema> schemas = new HashMap<String, Schema>();
	private Map<String, Table> tables = new HashMap<String, Table>();
	private String dataPath;

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public static DataManager getInstance() {
		if (instance == null)
			instance = new DataManager();
		return instance;
	}

	public void addNewTable(Table table) {
		tables.put(table.getName(), table);
	}

	public Schema getSchema(String tableName) {
		return tables.get(tableName).getSchema();
	}

	public Table getTable(String tableName) {
		return tables.get(tableName);
	}

}