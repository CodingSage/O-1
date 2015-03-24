package edu.buffalo.cse562.query.operators;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.Evaluator;

public class SelectOperator extends Operator {

	private Expression condition;

	public SelectOperator(Table t, Expression exp) {
		table = t;
		condition = exp;
	}

	protected Table evaluate() {
		Evaluator eval = new Evaluator(table);
		eval.reset();
		while (eval.hasNext()) 
		{
			try 
			{
				eval.next();
				LeafValue val = eval.eval(condition);
				if (!((BooleanValue) val).getValue())
					eval.remove();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
		return table;
	}

}
