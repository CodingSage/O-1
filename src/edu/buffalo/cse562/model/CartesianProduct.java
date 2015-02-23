package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

public class CartesianProduct {

	private List<Table> mytable;
	private List<List<Tuple>> data;
	private List<Tuple> output = new ArrayList<Tuple>();
	private int siz;
	private Schema resSchema = null;

	public CartesianProduct(List<Table> tables) {
		data = new ArrayList<List<Tuple>>();
		mytable = tables;
		siz = tables.size();
		resSchema = new Schema();
		for (int i = 0; i < tables.size(); i++) 
		{
			data.add(tables.get(i).getRows());
			// Adding the tables
			addToCartesianSchema(tables.get(i).getSchema());
		}
	}

	private void addToCartesianSchema(Schema objSchema) {
		for (int i = 0; i < objSchema.getColName().size(); i++) {
			resSchema.addColumn(objSchema.getColName().get(i), objSchema
					.getColType().get(i));
		}
	}

	public void CalculateCartesianProduct(int cur, Tuple tillnow) {
		if (cur == siz) {
			output.add(tillnow);
			return;
		}

		List<Tuple> tmp = data.get(cur);

		for (int i = 0; i < tmp.size(); i++) {
			Tuple ttillnow = tillnow;
			ttillnow = Tuple.merge(tillnow, tmp.get(i));
			CalculateCartesianProduct(cur + 1, ttillnow);
		}
		return;
	}

	public List<Table> getMytable() {
		return mytable;
	}

	public void setMytable(List<Table> mytable) {
		this.mytable = mytable;
	}

	public List<List<Tuple>> getData() {
		return data;
	}

	public void setData(List<List<Tuple>> data) {
		this.data = data;
	}

	public List<Tuple> getOutput() {
		CalculateCartesianProduct(0, new Tuple());
		return output;
	}

	public Schema getResSchema() {
		return resSchema;
	}

	public void setResSchema(Schema resSchema) {
		this.resSchema = resSchema;
	}

	public void setOutput(List<Tuple> output) {
		this.output = output;
	}

	public int getSiz() {
		return siz;
	}

	public void setSiz(int siz) {
		this.siz = siz;
	}

}
