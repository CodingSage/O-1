package edu.buffalo.cse562.model;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class Tuple {

	Table mytable;
	String[] mytuple;

	public Tuple(Table table, String tuple) {
		this.mytuple = tuple.split("\\|");
		this.mytable = table;
	}

	public String getTableName() {
		return mytable.getWholeTableName();
	}

	public String[] getTupleValue() {
		return mytuple;
	}

	public void displayTuple() {
		System.out.println("Tuple: " + mytuple);
	}

	public String getAlias() {
		if (mytable.getAlias() == null)
			return mytable.getWholeTableName();
		return mytable.getAlias();
	}

	public String getColumnValue(String column) {
		if (column.contains(".")) {
			String tmp = column.split("\\.")[0];
			column = column.split("\\.")[1];
			if (this.getAlias().compareToIgnoreCase(tmp) == 0)
				return null;
		}
		int index = 0;
		for (CreateTable createtable : Main.createtable) {
			String name1 = createtable.getTable().toString();
			String name2 = mytable.getWholeTableName().toString();
			if (name1.compareToIgnoreCase(name2) == 1) {
				for (Object columdefinitions : createtable
						.getColumnDefinitions()) {
					String currentcolumn = ((ColumnDefinition)columdefinitions).getColumnName();

					if (currentcolumn.contains(column))
						return mytuple[index];
					index += 1;
				}
			}
		}
		return null;
	}

	public String getOutPutSchemaColumnValue(String column) {
		if (column.contains(".")) {
			// String tmp = column.split("\\.")[0];
			column = column.split("\\.")[1];
			// if(this.getAlias().compareToIgnoreCase(tmp) == 0)
			// return null;
		}
		int index = 0;
		CreateTable createtable = Schema;
		String name1 = createtable.getTable().toString();
		String name2 = mytable.getWholeTableName().toString();
		if (name1.compareToIgnoreCase(name2) == 1) {
			for (Object columdefinitions : createtable.getColumnDefinitions()) {
				String currentcolumn = ((ColumnDefinition)columdefinitions).getColumnName();
				if (currentcolumn.contains(column))
					return mytuple[index];
				index += 1;
			}
		}
		return null;
	}

}
