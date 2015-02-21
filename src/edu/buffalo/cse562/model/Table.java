package edu.buffalo.cse562.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.core.DataManager;

public class Table {

	public List<Tuple> rows;
	public String name;

	public Table() {
		rows = new ArrayList<Tuple>();
	}
	
	public void addRow(Tuple tuple){
		rows.add(tuple);
	}

	public Table(String tableName) {
		rows = new ArrayList<Tuple>();
		this.setName(tableName);
		File file = new File(DataManager.getInstance().getDataPath()
				+ File.separator + tableName + ".dat");
		try {
			FileReader fileread = new FileReader(file);
			BufferedReader reader = new BufferedReader(fileread);
			String line;
			Tuple row = null;
			String[] datas;
			while ((line = reader.readLine()) != null) {
				row = new Tuple();
				datas = line.split("\\|");
				if (datas.length > 0) {
					for (String col : datas)
						row.insertColumn(col);
				}
				rows.add(row);
			}
			reader.close();
			fileread.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<Tuple> getRows() {
		return rows;
	}

	public void setRows(List<Tuple> rows) {
		this.rows = rows;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ArrayList<String> appendTuples(Tuple X, Tuple Y) {
		List<String> retTuple = new ArrayList<String>();
		return (ArrayList<String>) retTuple;
	}

	public void append(Table table) {
		rows.addAll(table.getRows());
	}

	@Override
	public String toString() {
		String s = "";
		if (rows == null)
			return s;
		int cnt = 0;
		for (Tuple r : rows) {
			if (cnt > 0)
				s += "\n" + r.toString();
			else
				s += r.toString();
			cnt++;
		}
		return s;
	}
}
