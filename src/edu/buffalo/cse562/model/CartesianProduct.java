package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.core.DataManager;

public class CartesianProduct {

	private List<Table> mytable;
	private List<ArrayList<Tuple>> data = new ArrayList<ArrayList<Tuple>>();
	private List<Tuple> output = new ArrayList<Tuple>();
	private int siz;
	private Schema resSchema = null;
	
	public CartesianProduct(List<Table> tables) {
		mytable = tables;
		siz = tables.size();
		resSchema = new Schema();
		for (int i = 0; i < tables.size(); i++){
			data.add((ArrayList<Tuple>) tables.get(i).rows);
			//Adding the tables
			addToCartesianSchema(DataManager.getInstance().getSchema(tables.get(i).getName()));
		}
		
	}
	private void addToCartesianSchema(Schema objSchema){
		
	   for(int i = 0;i<objSchema.getColName().size();i++){
		   resSchema.addColumn(objSchema.getColName().get(i),objSchema.getColType().get(i));
	   }
		
	}
	
	public void CalculateCartesianProduct(int cur, Tuple tillnow) {
		if (cur == siz) {
			tillnow.reverse();
			output.add(tillnow);
			return;
		}

		ArrayList<Tuple> tmp = data.get(cur);

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

	public List<ArrayList<Tuple>> getData() {
		return data;
	}

	public void setData(List<ArrayList<Tuple>> data) {
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
