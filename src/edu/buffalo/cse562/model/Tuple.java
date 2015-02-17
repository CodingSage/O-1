package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Tuple {

	List<String> tupleVal;

	public static Tuple merge(Tuple row1, Tuple row2) {
		List<String> retRow = new ArrayList<String>();
		if (row1 != null)
			retRow.addAll(row1.getTupleValue());
		if (row2 != null)
			retRow.addAll(row2.getTupleValue());
		Tuple newTuple = new Tuple(retRow);
		return newTuple;
	}

	public Tuple() {
		tupleVal = new ArrayList<String>();
	}

	public Tuple(List<String> row) {
		tupleVal = new ArrayList<String>(row);
	}

	public ArrayList<String> getTupleValue() {
		return (ArrayList<String>) tupleVal;
	}

	public void insertColumn(String data) {
		this.tupleVal.add(data);

	}

	public String getValue(int index) {
		return tupleVal.get(index);
	}

	public void reverse() {
		if (tupleVal != null && tupleVal.size() != 0)
			Collections.reverse(tupleVal);
	}

	@Override
	public String toString() {
		String s = "";
		int cnt = 0;
		for (String str : tupleVal) {
			if(cnt > 0)s += "|" + str; 	
			else s += str;
			cnt++;
		}
		return s;
	}
}
