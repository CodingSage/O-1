package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

public class Schema {

	private List<String> colName;
	private List<String> colType;

	public Schema() {
		colName = new ArrayList<String>();
		colType = new ArrayList<String>();
	}

	public void addColumn(String name, String type) {
		colName.add(name);
		colType.add(type);
	}

	public String getType(String name) {
		int index = getColIndex(name);
		return colType.get(index);
	}

	public int getColIndex(String col) {
		int index = colName.indexOf(col);
		if (index == -1)
			for (int j = 0; j < colName.size(); j++)
				if (colName.get(j).endsWith(col)) {
					index = j;
					break;
				}
		return index;
	}

	public int getNumberColumns() {
		return colName.size();
	}

	public List<String> getColName() {
		return colName;
	}

	public List<String> getColType() {
		return colType;
	}
}
