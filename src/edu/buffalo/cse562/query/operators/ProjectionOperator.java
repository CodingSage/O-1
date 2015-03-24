package edu.buffalo.cse562.query.operators;

import java.util.List;

import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;

public class ProjectionOperator extends Operator {

	private List<Target> targets;
	
	public ProjectionOperator(Table t, List<Target> cols) {
		table = t;
		targets = cols;
	}
	
	@Override
	protected Table evaluate() {
		// TODO projection implementations
		return table;
	}

}
