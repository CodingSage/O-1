package edu.buffalo.cse562.core;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;

public class DataManager {

	private static DataManager instance;
	private Map<String, Table> tables = new HashMap<String, Table>();
	private String dataPath;
	private String storagePath;
	private static int ansNum = -1;

	public static DataManager getInstance() {
		if (instance == null)
			instance = new DataManager();
		return instance;
	}
	
	public int getTableCount(){
		return tables.size();
	}

	public String assignFileName() {
		if (storagePath == null || storagePath.isEmpty())
			return "";
		return storagePath + File.separatorChar + (++ansNum) + ".dat";
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
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

	public void setStoragePath(String path) {
		storagePath = path;
	}

	public String getStoragePath() {
		return storagePath == null ? "" : storagePath;
	}

}