package edu.buffalo.cse562.model;

import java.util.List;

import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.PlanNode;

public class JoinNode extends PlanNode.Binary {

	private List<Column> cols;
	
	public JoinNode(List<Column> conditionCols) {
		cols = conditionCols;
	}

	@Override
	public String detailString() {
		return "{\n JOIN [" + cols.get(0) + " = " + cols.get(1) + "\n}";
	}

	@Override
	public List<Column> getSchemaVars() {
		return null;
	}

}
