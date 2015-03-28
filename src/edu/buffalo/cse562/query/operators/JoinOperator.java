package edu.buffalo.cse562.query.operators;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.CalculateJoin;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.Evaluator;

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
		// TODO Auto-generated method stub
		String full = expr.toString();
		String columns[] = expr.toString().split(" = ");
		
		CalculateJoin c = new CalculateJoin(table, table2, columns[0], columns[1]);
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
