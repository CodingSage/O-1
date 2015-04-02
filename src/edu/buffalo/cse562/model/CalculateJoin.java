package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse562.core.DataManager;

public class CalculateJoin {
	
	private Table TableLeft, TableRight;
	private int indleft, indright;
	private String ColumnNameLeft, ColumnNameRight;
	private Table ResultTable;
	
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

	public Table getResultTable() {
		return InMemoryEfficientJoin(TableLeft, TableRight); 
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
	public Table InMemoryJoin(Table t1, Table t2) {
	
		List<Tuple> l1 = t1.getRows();
		List<Tuple> l2 = t2.getRows();
		
		ResultTable = new Table();
		
		ResultTable.setName(t1.getName() + t2.getName());
		
		
		Schema s = new Schema();
		s.addSchema(t1.getSchema());
		s.addSchema(t2.getSchema());
		ResultTable.setSchema(s);
		
		Tuple tmp1, tmp2; 
		
		for(Tuple tupl: l1)
		{
				for(Tuple tupr: l2)
				{
						if(tupl.getValue(indleft).compareTo(tupr.getValue(indright)) == 0)
						{
								tmp1 = tupl;
								tmp2 = tupr;
								ResultTable.addRow(tmp1.merge(tmp1, tmp2));	
						}
				}
		}
		return ResultTable;
	}

	public Table InMemoryEfficientJoin(Table t1, Table t2)
	{
		List<Tuple> l1 = t1.getRows();
		List<Tuple> l2 = t2.getRows();
	
		ResultTable = new Table();
		
		Map< String, List<Tuple> > hash = new HashMap<String, List<Tuple> >();
		
		ResultTable.setName(t1.getName() + t2.getName());
		
		Schema s = new Schema();
		s.addSchema(t1.getSchema());
		s.addSchema(t2.getSchema());

		ResultTable.setSchema(s);
		
		
		for(Tuple tupl : l1)
		{
			if(hash.get(tupl.getValue(indleft)) == null)
			{
				List<Tuple> tmp = new ArrayList<Tuple>();
				tmp.add(tupl);
				hash.put(tupl.getValue(indleft), tmp);
			}
			else
			{	
				List<Tuple> tmp = hash.get(tupl.getValue(indleft));
				tmp.add(tupl);
				hash.put(tupl.getValue(indleft), tmp);
			}
		}	
		
		Tuple tmp1, tmp2; 
		
		for(Tuple tupr : l2)
		{
				tmp1 = tupr;
				
				if(hash.get(tupr.getValue(indright)) != null)
				{
					List<Tuple> tmp = hash.get(tupr.getValue(indright));
					
					
					for(Tuple tupl : tmp)
					{
						tmp2 = tupl;
						ResultTable.addRow(tmp2.merge(tmp2, tmp1));
					}
				}
			
		}
		
		return ResultTable;
				
	}
	
}
