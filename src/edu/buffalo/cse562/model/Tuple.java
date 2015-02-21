package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

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

	public String eval(Column x){
		
		int colID =  1;
		return tupleVal.get(colID);
		
	}
	
	public String getValue(int index) {
		return tupleVal.get(index);
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
