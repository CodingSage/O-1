package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.core.DataManager;

public class CalculateJoin {

	private Table TableLeft, TableRight;
	private int indleft, indright;
	private String ColumnNameLeft, ColumnNameRight;
	private Table ResultTable;
	private Expression expression;

	public CalculateJoin(Table tl, Table tr, String colleft, String colright, Expression exp) {
		TableLeft = tl;
		TableRight = tr;
		expression = exp;
		ColumnNameLeft = colleft;
		ColumnNameRight = colright;

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

		for (Tuple tupl : l1) {
			for (Tuple tupr : l2) {
				if (tupl.getValue(indleft).toString().compareTo(tupr.getValue(indright).toString()) == 0) {
					tmp1 = tupl;
					
					tmp2 = tupr;
					ResultTable.addRow(tmp1.merge(tmp1, tmp2));
				}
			}
		}
		return ResultTable;
	}
	
	private class Details{
		Map<String, List<Tuple>> hash = new HashMap<String, List<Tuple>>();
		Schema s = null;
		Schema s2 = null;
		int fg = 0;
	}
	
	private static Map<String, Details> details = new HashMap<String, CalculateJoin.Details>();
	
	/*private static Map<String, List<Tuple>> hash = new HashMap<String, List<Tuple>>();
	private static Schema s = null;
	private static Schema s2 = null;
	private static int fg = 0;*/

	public Table InMemoryEfficientJoin(Table t1, Table t2) 
	{
		
		if(t1 == null && t2 == null)
		{
			if(details.containsKey(expression.toString()))
				details.remove(expression.toString());
			/*s = null;
			s2 = null;
			hash = new HashMap<String, List<Tuple>>();
			fg = 0;*/
			return null;
		}
		
		if(!details.containsKey(expression.toString()))
			details.put(expression.toString(), new Details());
		Details info = details.get(expression.toString());
		
		if(t2 == null)
		{
			
			if(info.s == null)
			{
				info.s = t1.getSchema();
			}
			indleft = info.s.getColIndex(ColumnNameLeft);
		
			Tuple tupl = t1.getRows().get(0);
			
			if (!info.hash.containsKey(tupl.getValue(indleft)))
			{
				List<Tuple> tmp = new ArrayList<Tuple>();
				tmp.add(tupl);
				info.hash.put(tupl.getValue(indleft).toString(), tmp);
			} 
			else 
			{
				List<Tuple> tmp = info.hash.get(tupl.getValue(indleft));
				tmp.add(tupl);
				info.hash.put(tupl.getValue(indleft).toString(), tmp);
			}
		
			return new Table();
		}
		else if(t1 == null)
		{
			
			Table ResultTable = new Table();
			
			if(info.s2 == null)
			{
				info.s2 = t2.getSchema();
				info.s.addSchema(info.s2);
			}
			indright = info.s2.getColIndex(ColumnNameRight);
		
			ResultTable.setSchema(info.s);
			
			
			Tuple tupr = t2.getRows().get(0);
			Tuple tmp1 = new Tuple();
		
			
			String value = tupr.getValue(indright).toString();
			
			if (info.hash.containsKey(value)) 
			{
					List<Tuple> tmp = info.hash.get(value);
					for (Tuple tupl : tmp) 
					{
						tmp1 = tupl;
						ResultTable.addRow(tmp1.merge(tmp1, tupr));
					}
					return ResultTable;
			}
			return ResultTable;

		}

		return new Table();

	}

}
