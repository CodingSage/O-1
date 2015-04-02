package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.PlanNode;

public class JoinNode extends PlanNode.Binary {

	private BinaryExpression exp;

	public JoinNode(BinaryExpression expression) {
		exp = expression;
	}

	@Override
	public String detailString() {
		return "JOIN [" + exp.getLeftExpression() + " = " + exp.getRightExpression() + "]";
	}

	@Override
	public List<Column> getSchemaVars() {
		List<Column> cols = new ArrayList<Column>();
		cols.addAll(getLHS().getSchemaVars());
		cols.addAll(getRHS().getSchemaVars());
		return cols;
	}

	public Expression getCondition() {
		return exp;
	}

}
