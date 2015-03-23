package edu.buffalo.cse562.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.Constants;
import edu.buffalo.cse562.core.DataManager;

public class Table {

	private Schema schema;
	private List<Tuple> rows;
	private String name;

	public Table() {
		schema = new Schema();
		rows = new ArrayList<Tuple>();
	}

	public Table(Schema schema) {
		this.schema = schema;
	}

	public Table(String singleVal, String valType) {
		schema = new Schema();
		schema.addColumn(Constants.COLNAME_DEFAULT, valType);
		rows = new ArrayList<Tuple>();
		Tuple t = new Tuple();
		t.insertColumn(singleVal);
		rows.add(t);
	}

	public Table(String tableName) {
		name = tableName;
	}

	public void loadData() {
		// TODO check loading conditions on memory constraints
		rows = new ArrayList<Tuple>();
		String tableName = name;
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
			name = tableName.toLowerCase();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean isEmpty() {
		return rows.isEmpty();
	}

	public String getValue(int rowIndex, int colIndex) {
		return rows.get(rowIndex).getValue(colIndex);
	}

	public void addTableColumn(Table col) {
		if (rows.size() == 0)
			rows = col.getRows();
		else {
			for (int i = 0; i < rows.size(); i++) {
				rows.get(i).insertColumn(col.getValue(i, 0));
			}
		}
	}

	public void addRow(Tuple tuple) {
		rows.add(tuple);
	}

	public ArrayList<String> appendTuples(Tuple X, Tuple Y) {
		List<String> retTuple = new ArrayList<String>();
		return (ArrayList<String>) retTuple;
	}

	public void append(Table table) {
		rows.addAll(table.getRows());
	}

	public Table getColumn(String colName) {
		colName = colName.toLowerCase();
		List<Tuple> t = new ArrayList<Tuple>();
		try {
			int ci = schema.getColIndex(colName);
			for (int i = 0; i < rows.size(); i++) {
				Tuple tup = new Tuple();
				tup.insertColumn(rows.get(i).getValue(ci));
				t.add(tup);
			}
		} catch (Exception ex) {
			try {
				String st = "";
				for (String s : this.schema.getColName()) {
					st += s + ",";
				}
				throw new Exception("||" + st + " " + this.name + " " + colName
						+ "\n" + ex.getMessage());
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
		Table tab = new Table();
		tab.setRows(t);
		return tab;
	}

	public int columnsCount() {
		if (rows.size() != 0)
			if (rows.get(0).getValues().size() != 0)
				return rows.get(0).getValues().size();
		return 0;
	}

	public void removeRow(int index) {
		rows.remove(index);
	}

	public void addRowAt(Tuple t, int index) {
		rows.add(index, t);
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

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
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

}
