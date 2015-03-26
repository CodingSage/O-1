package edu.buffalo.cse562.query.operators;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;

public class JoinOperator extends Operator {

	private Table table2;
	private Expression expr;

	public JoinOperator(Table t, Table t2, Expression exp) {
		table = t;
		table2 = t2;
		expr = exp;
	}

	@Override
	protected Table evaluate() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Table execute() {
		// TODO Auto-generated method stub
		return super.execute();
	}

}
