package edu.buffalo.cse562.query.operators;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.CalculateJoin;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;

public class JoinOperator extends Operator {

	private Table table, table2;
	private Expression expr;

	public JoinOperator(Table t, Table t2, Expression exp) {
		table = t;
		table2 = t2;
		expr = exp;
	}

	@Override
	protected Table evaluate() {
		if(table != null && table.isEmpty())
			return new Table();
		if(table2 != null && table2.isEmpty())
			return new Table();
		BinaryExpression bexpr = ((BinaryExpression)expr);
		String col0 = ((Column)bexpr.getLeftExpression()).getWholeColumnName();
		String col1 = ((Column)bexpr.getRightExpression()).getWholeColumnName();
		//if(col0.startsWith("L")){
		if(table2 != null && table2.containsColumn(col0)){
			String temp = col0;
			col0 = col1;
			col1 = temp;
		}
		
		if(table != null && table.containsColumn(col1))
		{
			String temp = col1;
			col1 = col0;
			col0 = temp;
		}
		/*String columns = expr.toString();
		String col0 = columns.substring(0, columns.indexOf('=')).trim();
		String col1 = columns.substring(columns.indexOf('=')+1).trim();*/
		CalculateJoin c = new CalculateJoin(table, table2, col0, col1, expr);
		return c.InMemoryEfficientJoin(table, table2);
	}
	
	@Override
	public Table execute() {
		// TODO Auto-generated method stub
		return super.execute();
	}

}
