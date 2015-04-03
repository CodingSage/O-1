package edu.buffalo.cse562.query.operators;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;

public class SelectLoadOperator extends Operator {

	private Expression condition;

	public SelectLoadOperator(Table t, Expression exp) {
		table = t;
		condition = exp;
	}
	
	@Override
	protected Table evaluate() {
		table.loadData(condition);
		return table;
	}

}
