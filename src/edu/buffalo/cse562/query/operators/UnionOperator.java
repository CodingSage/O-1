package edu.buffalo.cse562.query.operators;

import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;

public class UnionOperator extends Operator {

	private Table table2;
	
	public UnionOperator(Table a, Table b) {
		table = a;
		table2 = b;
	}
	
	@Override
	protected Table evaluate() {
		table.append(table2);
		return table;
	}

}
