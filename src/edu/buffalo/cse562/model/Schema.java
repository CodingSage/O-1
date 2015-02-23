package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.Constants;

public class Schema {

	private List<String> colName;
	private List<String> colType;

	public Schema() {
		colName = new ArrayList<String>();
		colType = new ArrayList<String>();
	}

	public void addColumn(String name, String type) {
		name = name.toLowerCase();
		colName.add(name);
		colType.add(type);
	}

	public String getType(String name) {
		name = name.toLowerCase();
		int index = getColIndex(name);
		return colType.get(index);
	}

	public int getColIndex(String col) {
		col = col.toLowerCase();
		int index = colName.indexOf(col);
		if (index == -1)
			for (int j = 0; j < colName.size(); j++) {
				String c = colName.get(j);
				if (c.contains(Constants.AGGREGATE_INDICATOR)) {
					c = c.split(Constants.AGGREGATE_INDICATOR)[0];
					if (col.equals(c)) {
						index = j;
						break;
					}
				} else if (c.endsWith(col)) {
					index = j;
					break;
				}
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

	public void setColName(List<String> colNames) {
		this.colName = colNames;
		for (int i = 0; i < colName.size(); i++)
			colName.set(i, colName.get(i).toLowerCase());
	}
}
