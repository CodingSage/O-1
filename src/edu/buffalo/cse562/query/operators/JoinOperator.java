package edu.buffalo.cse562.query.operators;

import net.sf.jsqlparser.expression.Expression;
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
		String columns = expr.toString();//.split(" = ");
		String col0 = columns.substring(0, columns.indexOf('='));
		String col1 = columns.substring(columns.indexOf('=')+1);
		CalculateJoin c = new CalculateJoin(table, table2, col0, col1);
		Table reslt = c.InMemoryJoin(table, table2);
		DataManager.getInstance().addNewTable(reslt);
		return reslt;
	}
	
	@Override
	public Table execute() {
		// TODO Auto-generated method stub
		return super.execute();
	}

}
