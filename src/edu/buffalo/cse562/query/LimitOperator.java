package edu.buffalo.cse562.query;

import java.util.List;

import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;

public class LimitOperator extends Operator {

	private long tableCount;

	public LimitOperator(long count, Table a) {
		table = a;
		tableCount = count;
	}

	@Override
	protected Table evaluate() {
		Table res = new Table();
		List<Tuple> rows = table.getRows();
		for (int i = 0; i < tableCount; i++)
			res.addRow(rows.get(i));
		res.setSchema(table.getSchema());
		return res;
	}

}
