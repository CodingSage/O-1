package edu.buffalo.cse562.query;

import edu.buffalo.cse562.checkpoint1.LimitNode;
import edu.buffalo.cse562.checkpoint1.PlanNode;
import edu.buffalo.cse562.checkpoint1.ProductNode;
import edu.buffalo.cse562.checkpoint1.ProjectionNode;
import edu.buffalo.cse562.checkpoint1.SelectionNode;
import edu.buffalo.cse562.checkpoint1.SortNode;
import edu.buffalo.cse562.checkpoint1.UnionNode;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.operators.SelectOperator;

public class Query {

	private PlanNode raTree;

	public Query(PlanNode query) {
		this.raTree = query;
	}

	public void evaluate() {
		optimizeTree(raTree);
		Table result = evaluateTree(raTree);
		if (result != null) {
			System.out.print(result.toString());
		}
	}

	private Table evaluateTree(PlanNode tree) {
		Table res = new Table();
		if (tree instanceof PlanNode.Unary) {
			Table a = evaluateTree(((PlanNode.Unary) tree).getChild());
			res = evaluate((PlanNode.Unary) tree, a);
		} else if (tree instanceof PlanNode.Binary) {
			PlanNode.Binary btree = (PlanNode.Binary) tree;
			Table a = evaluateTree(btree.getLHS());
			Table b = evaluateTree(btree.getRHS());
			res = evaluate(btree, a, b);
		} else {
			res = DataManager.getInstance().getTable(
					((PlanNode.Leaf) tree).toString());
		}
		return res;
	}

	private Table evaluate(PlanNode.Unary node, Table a) {
		Operator op = null;
		if (node instanceof LimitNode)
			;
		else if (node instanceof ProjectionNode)
			;
		else if (node instanceof SelectionNode)
			op = new SelectOperator(a, ((SelectionNode) node).getCondition());
		else if (node instanceof SortNode)
			;
		else
			;// Aggregate
		return op.execute();
	}

	private Table evaluate(PlanNode.Binary node, Table a, Table b) {
		Operator op = null;
		if (node instanceof ProductNode)
			;
		else if (node instanceof UnionNode)
			;
		else
			;// Joins
		return op.execute();
	}

	private void optimizeTree(PlanNode tree) {

	}

}
