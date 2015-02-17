package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.core.DataManager;

public class CalculateJoin {
	
	private Table TableLeft, TableRight;
	private int indleft, indright;
	private String ColumnNameLeft, ColumnNameRight;
	private List<Tuple> output;
	
	public CalculateJoin(Table tl, Table tr, String colleft, String colright) 
	{
		TableLeft = tl;
		TableRight = tr;
		
		ColumnNameLeft = colleft;
		ColumnNameRight = colright;
		
		Schema schema1 = DataManager.getInstance().getSchema(TableLeft.getName());
		indleft = schema1.getColIndex(colleft);
		
		Schema schema2 = DataManager.getInstance().getSchema(TableRight.getName());
		indright = schema2.getColIndex(colright);

	}

	public Table getTableLeft() {
		return TableLeft;
	}

	public void setTableLeft(Table tableLeft) {
		TableLeft = tableLeft;
	}

	public Table getTableRight() {
		return TableRight;
	}

	public void setTableRight(Table tableRight) {
		TableRight = tableRight;
	}

	public List<Tuple> getResultTable() {
		CalculateConditionalJoin(TableLeft, TableRight); 
		return output;
	}

	public void setResultTable(List<Tuple> resultTable) {
		output = resultTable;
	}

	public int getIndleft() {
		return indleft;
	}

	public void setIndleft(int indleft) {
		this.indleft = indleft;
	}

	public int getIndright() {
		return indright;
	}

	public void setIndright(int indright) {
		this.indright = indright;
	}

	public String getColumnNameLeft() {
		return ColumnNameLeft;
	}

	public void setColumnNameLeft(String columnNameLeft) {
		ColumnNameLeft = columnNameLeft;
	}

	public String getColumnNameRight() {
		return ColumnNameRight;
	}

	public void setColumnNameRight(String columnNameRight) {
		ColumnNameRight = columnNameRight;
	}

	@SuppressWarnings("static-access")
	public void CalculateConditionalJoin(Table t1, Table t2) {
	
		List<Tuple> l1 = t1.getRows();
		List<Tuple> l2 = t2.getRows();
		
		output = new ArrayList<Tuple>(); 
		
		for(Tuple tupl: l1)
		{
				for(Tuple tupr: l2)
				{
						if(tupl.getValue(indleft).compareTo(tupr.getValue(indright)) == 0)
						{
								output.add(tupl.merge(tupl, tupr));	
						}
				}
		}
		
		
			
		return ;
	}

	
}
