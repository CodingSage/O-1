package edu.buffalo.cse562.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Table {

	private String name;
	private List<String> rows;

	public Table() {
		rows = new ArrayList<String>();
	}

	public Table(String tableName) {
		rows = new ArrayList<String>();
		setName(tableName);
		File file = new File("/home/vinayak/CourseWork/Databases/Sanity_Check_Examples/data/r.dat");
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line;
			while ((line = reader.readLine()) != null) {
				rows.add(line);
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getStringElement(int row, int col) {
		return rows.get(row).split("|")[col];
	}

	public int getIntElement(int row, int col) {
		return Integer.parseInt(rows.get(row).split("|")[col]);
	}

	public Date getDateElement(int row, int col) {
		return new Date(Date.parse(rows.get(row).split("|")[col]));
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public String toString() {
		String res = "";
		for(String str : rows){
			res += str + "\n";
		}
		return res;
	}

}
