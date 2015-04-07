package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

public class Tuple {

	List<LeafValue> tupleVal;

	public Tuple() {
		tupleVal = new ArrayList<LeafValue>();
	}

	public Tuple(int capacity) {
		tupleVal = new ArrayList<LeafValue>();
		for (int i = 0; i < capacity; i++)
			tupleVal.add(new StringValue(""));
	}

	public static Tuple merge(Tuple row1, Tuple row2) {
		List<LeafValue> retRow = new ArrayList<LeafValue>();
		if (row1 != null)
			retRow.addAll(row1.getValues());
		if (row2 != null)
			retRow.addAll(row2.getValues());
		Tuple newTuple = new Tuple(retRow);
		return newTuple;
	}

	public Tuple(List<LeafValue> row) {
		tupleVal = new ArrayList<LeafValue>(row);
	}

	public List<LeafValue> getValues() {
		return tupleVal;
	}

	public void insertColumn(LeafValue data) {
		this.tupleVal.add(data);
	}

	public LeafValue eval(Column x) {
		int colID = 1;
		return tupleVal.get(colID);
	}

	public LeafValue getValue(int index) {
		return tupleVal.get(index);
	}

	public void setValue(int index, LeafValue value) {
		if (index < tupleVal.size())
			tupleVal.set(index, value);
	}

	@Override
	public String toString() {
		String s = "";
		int cnt = 0;
		for (LeafValue str : tupleVal) {
			String s1 = "";
			if(str instanceof StringValue)
				s1 = ((StringValue)str).getValue();
			else if(str instanceof DoubleValue)
				s1 = ((DoubleValue)str).getValue() + "";
			if(str instanceof LongValue)
				s1 = ((LongValue)str).getValue() + "";
			if(str instanceof DateValue)
				s1 = ((DateValue)str).getValue() + "";
			if(s1.contains("'"))
				s1= s1.replace("'", "");
			if (cnt > 0)
				s += "|" + s1;
			else
				s += s1;
			cnt++;
		}
		return s;
	}
}