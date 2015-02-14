package edu.buffalo.cse562.model;



import java.util.ArrayList;
import java.util.List;

public class Tuple  {

	Table mytable;
	String[] mytuple;
    List<String> tupleVal  ;
   
    
    public static Tuple merge(Tuple row1,Tuple row2){
    	
    	List<String> retRow = new ArrayList<String>();
    	if(row1!= null)
    		retRow.addAll(row1.getTupleValue());
    	if(row2!= null) retRow.addAll(row2.getTupleValue());	
    	
    	Tuple newTuple = new Tuple(retRow);
    	return newTuple;
    }
    
    public Tuple(){
    	tupleVal = new ArrayList<String>();
    }
    
    public Tuple(List<String> row){
    	
    	tupleVal  = new ArrayList<String>(row);
    	
    }
    
    public Tuple(Table table, String tuple) {
		this.mytuple = tuple.split("\\|");
		//this.mytable = table;
	}

     //	public String getTableName() {
     //		return mytable.getWholeTableName();
     //	}

	public ArrayList<String> getTupleValue() {
		return (ArrayList<String>) tupleVal;
	}

	public void insertColumn(String data){
		this.tupleVal.add(data);
		
	}
	public void displayTuple() {
		System.out.println("Tuple: " + mytuple);
	}

	
//	public String getAlias() {
//		if (mytable.getAlias() == null)
//			return mytable.getWholeTableName();
//		return mytable.getAlias();
//	}

//	public String getColumnValue(String column) {
//		if (column.contains(".")) {
//			String tmp = column.split("\\.")[0];
//			column = column.split("\\.")[1];
//			if (this.getAlias().compareToIgnoreCase(tmp) == 0)
//				return null;
//		}
//		int index = 0;
//		for (CreateTable createtable : Main.createtable) {
//			String name1 = createtable.getTable().toString();
//			String name2 = mytable.getWholeTableName().toString();
//			if (name1.compareToIgnoreCase(name2) == 1) {
//				for (Object columdefinitions : createtable
//						.getColumnDefinitions()) {
//					String currentcolumn = ((ColumnDefinition)columdefinitions).getColumnName();
//
//					if (currentcolumn.contains(column))
//						return mytuple[index];
//					index += 1;
//				}
//			}
//		}
//		return null;
//	}

//	public String getOutPutSchemaColumnValue(String column) {
//		if (column.contains(".")) {
//			// String tmp = column.split("\\.")[0];
//			column = column.split("\\.")[1];
//			// if(this.getAlias().compareToIgnoreCase(tmp) == 0)
//			// return null;
//		}
//		int index = 0;
//		CreateTable createtable = Schema;
//		String name1 = createtable.getTable().toString();
//		String name2 = mytable.getWholeTableName().toString();
//		if (name1.compareToIgnoreCase(name2) == 1) {
//			for (Object columdefinitions : createtable.getColumnDefinitions()) {
//				String currentcolumn = ((ColumnDefinition)columdefinitions).getColumnName();
//				if (currentcolumn.contains(column))
//					return mytuple[index];
//				index += 1;
//			}
//		}
//		return null;
//	}

}
