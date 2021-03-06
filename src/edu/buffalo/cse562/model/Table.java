package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.Constants;

public class Table {

	private Schema schema;
	private List<Tuple> rows;
	private String name;
	public Map<String, Integer> FilesList = new HashMap<String, Integer>();

	public Table() {
		schema = new Schema();
		rows = new ArrayList<Tuple>();
	}

	public Table(Schema schema) {
		this.schema = schema;
		rows = new ArrayList<Tuple>();
	}

	public Table(LeafValue singleVal, ColumnType valType) {
		schema = new Schema();
		schema.addColumn(Constants.COLNAME_DEFAULT, valType);
		rows = new ArrayList<Tuple>();
		Tuple t = new Tuple();
		t.insertColumn(singleVal);
		rows.add(t);
	}

	public Table(LeafValue singleVal, ColumnType valType, String colName) {
		schema = new Schema();
		schema.addColumn(colName, valType);
		rows = new ArrayList<Tuple>();
		Tuple t = new Tuple();
		t.insertColumn(singleVal);
		rows.add(t);
	}

	public Table(String tableName) {
		name = tableName;
		rows = new ArrayList<Tuple>();
	}
	
	public boolean containsColumn(String col){
		Integer i = schema.getColIndex(col);
		return i != null;
	}

	public void loadData() {
		/*rows = new ArrayList<Tuple>();
		String tableName = name;
		File file = new File(DataManager.getInstance().getDataPath()
				+ File.separator + tableName + ".dat");
		try {
			// System.gc();
			FileReader fileread = new FileReader(file);
			BufferedReader reader = new BufferedReader(fileread);
			String line = null;
			// Tuple row = null;
			String[] datas;
			int k = 0;
			while ((line = reader.readLine()) != null) {
				datas = line.split("\\|");
				k = datas.length;
				Tuple row = new Tuple();
				for (int di = 0; di < k; di++)
					row.insertColumn(datas[di]);
				rows.add(row);
			}
			System.out.println("The number of tuples is :" + rows.size());
			reader.close();
			fileread.close();
			name = tableName.toLowerCase();
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}

	public void loadData(Expression exp) {
		/*rows = new ArrayList<Tuple>();
		String tableName = name;
		File file = new File(DataManager.getInstance().getDataPath()
				+ File.separator + tableName + ".dat");
		Table clone = new Table();
		clone.setSchema(schema);
		name = tableName.toLowerCase();
		try {
			FileReader fileread = new FileReader(file);
			BufferedReader reader = new BufferedReader(fileread);
			String line = null;
			Evaluator eval = new Evaluator(clone);
			String[] datas;
			Tuple row;
			LeafValue val;
			while ((line = reader.readLine()) != null) {
				datas = line.split("\\|");
				row = new Tuple(Arrays.asList(datas));
				clone.removeRow(0);
				clone.addRow(row);
				eval.reset();
				eval.next();
				val = eval.eval(exp);
				if (((BooleanValue) val).getValue())
					rows.add(row);
			}
			reader.close();
			fileread.close();
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}

	public boolean isEmpty() {
		return rows.isEmpty();
	}

	public LeafValue getValue(int rowIndex, int colIndex) {
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
		if(rows == null)
			rows = new ArrayList<Tuple>();
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
				e.printStackTrace();
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
		if (rows.size() > index)
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
