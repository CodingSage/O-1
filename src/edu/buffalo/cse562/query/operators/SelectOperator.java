package edu.buffalo.cse562.query.operators;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.Evaluator;

public class SelectOperator implements Operator {

	private Expression condition;
	private Table table;

	public SelectOperator(Table t, Expression exp) {
		table = t;
		condition = exp;
	}

	private Table evaluate() {
		Evaluator eval = new Evaluator(table);
		eval.reset();
		while (eval.hasNext()) {
			try {
				eval.next();
				LeafValue val = eval.eval(condition);
				if (val instanceof BooleanValue) {
					if (!((BooleanValue) val).getValue())
						eval.remove();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return table;
	}

	@Override
	public Table execute() {
		Table res = new Table();
		String name = DataManager.getInstance().assignFileName();
		if (name.isEmpty())
			res = evaluate();
		else {
			table.loadData();
			while (table.isEmpty()) {
				res.setName(name);
				res.append(evaluate());
			}
		}
		return res;
	}

}
