package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

public class CartesianProduct {

	private List<Table> mytable;
	private List<ArrayList<Tuple>> data;
	private List<ArrayList<Tuple>> output;
	private int siz;

	public CartesianProduct(List<Table> tables) {
		mytable = tables;
		siz = tables.size();
		for (int i = 0; i < tables.size(); i++)
			data.add((ArrayList<Tuple>) tables.get(i).rows);
	}

	public void CalculateCartesianProduct(int cur, ArrayList<Tuple> tillnow) {
		if (cur == siz) {
			output.add(tillnow);
			return;
		}

		ArrayList<Tuple> tmp = data.get(cur);

		for (int i = 0; i < tmp.size(); i++) {
			ArrayList<Tuple> ttillnow = tillnow;
			ttillnow.add(tmp.get(i));
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

	public List<ArrayList<Tuple>> getOutput() {
		return output;
	}

	public void setOutput(List<ArrayList<Tuple>> output) {
		this.output = output;
	}

	public int getSiz() {
		return siz;
	}

	public void setSiz(int siz) {
		this.siz = siz;
	}

}
