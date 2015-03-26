package edu.buffalo.cse562.query.operators;

import java.util.List;

import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.AggregateNode.AType;
import edu.buffalo.cse562.checkpoint1.AggregateNode.AggColumn;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;

public class AggregateOperator extends Operator {

	private List<AggColumn> aggregates;
	private List<Target> groups;

	public AggregateOperator(List<Target> groupCols, List<AggColumn> aggColumns, Table a) {
		groups = groupCols;
		table = a;
		aggregates = aggColumns;
	}

	@Override
	protected Table evaluate() {
		Table res = new Table();
		for (AggColumn agg : aggregates) {
			if(agg.aggType == AType.AVG)
				;
		}
		return res;
	}	

}
