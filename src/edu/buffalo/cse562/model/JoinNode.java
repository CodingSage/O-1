package edu.buffalo.cse562.model;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.PlanNode;

public class JoinNode extends PlanNode.Binary {

	Expression expr;

	public JoinNode(Expression exp) {
		expr = exp;
	}

	@Override
	public String detailString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Column> getSchemaVars() {
		// TODO Auto-generated method stub
		return null;
	}

	public Expression getExpression() {
		return expr;
	}

}
