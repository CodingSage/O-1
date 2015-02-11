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
	
	public String getType(String name){
		int index = colName.indexOf(name);
		return colType.get(index);
	}

}
