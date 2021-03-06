package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.buffalo.cse562.Constants;

public class Schema {

	private List<String> colName;
	private List<ColumnType> colType;
	private Map<String, Integer> colmap;
	private List<Integer> primaryKeyIndex;

	public Schema() {
		colName = new ArrayList<String>();
		colType = new ArrayList<ColumnType>();
		colmap = new HashMap<String, Integer>();
		primaryKeyIndex = new ArrayList<Integer>();
	}

	public void addColumn(String name, ColumnType type) {
		name = name.toUpperCase().trim();
		colmap.put(name, colName.size());
		colName.add(name);
		colType.add(type);
	}

	public ColumnType getType(String name) {
		int index = getColIndex(name);
		return colType.get(index);
	}

	public Integer getColIndex(String col) {
		col = col.trim();
		Integer i = colmap.get(col);
		if (i == null) {
			for (int j = 0; j < colName.size(); j++) {
				String c = Utilities.splitStrings('.', colName.get(j)).get(1);
				if (c.equals(col))
					return j;
			}
		}
		return i;
	}

	public void changeTableName(String tableName) {
		Iterator<Entry<String, Integer>> it = colmap.entrySet().iterator();
		Map<String, Integer> map = new HashMap<String, Integer>();
		while (it.hasNext()) {
			Entry<String, Integer> entry = it.next();
			String col = entry.getKey();
			col = col.substring(col.indexOf(Constants.COLNAME_DELIMITER) + 1);
			String newName = tableName + Constants.COLNAME_DELIMITER + col;
			map.put(newName, entry.getValue());
		}
		colmap = map;
	}

	public void addSchema(Schema schema) {
		colName.addAll(schema.getColName());
		colType.addAll(schema.getColType());
		for (String name : schema.getColName())
			colmap.put(name, colmap.size());
	}

	public int getNumberColumns() {
		return colName.size();
	}

	public List<String> getColName() {
		return colName;
	}

	public List<ColumnType> getColType() {
		return colType;
	}

	public void setColName(List<String> colNames) {
		this.colName = colNames;
		for (int i = 0; i < colName.size(); i++)
			colName.set(i, colName.get(i));
	}

	public void addPrimaryKey(String key) {
		primaryKeyIndex.add(getColIndex(key));
	}

	public List<Integer> getPrimaryKeyIndex() {
		return primaryKeyIndex;
	}
}
