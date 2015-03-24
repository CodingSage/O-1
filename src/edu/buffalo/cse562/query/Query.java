package edu.buffalo.cse562.query;

import java.util.List;

import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.LimitNode;
import edu.buffalo.cse562.checkpoint1.PlanNode;
import edu.buffalo.cse562.checkpoint1.ProductNode;
import edu.buffalo.cse562.checkpoint1.ProjectionNode;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.checkpoint1.SelectionNode;
import edu.buffalo.cse562.checkpoint1.SortNode;
import edu.buffalo.cse562.checkpoint1.UnionNode;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.operators.ProjectionOperator;
import edu.buffalo.cse562.query.operators.SelectOperator;
import edu.buffalo.cse562.query.operators.UnionOperator;

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
			String tableName = ((PlanNode.Leaf) tree).toString();
			int i = tableName.indexOf("[");
			int j = tableName.indexOf("(");
			tableName = tableName.substring(i + 1, j);
			res = DataManager.getInstance().getTable(tableName);
			res.loadData();
		}
		return res;
	}

	private Table evaluate(PlanNode.Unary node, Table a) {
		Operator op = null;
		if (node instanceof LimitNode)
			op = new LimitOperator(((LimitNode) node).getCount(), a);
		else if (node instanceof ProjectionNode) {
			List<Target> cols = ((ProjectionNode) node).getColumns();
			op = new ProjectionOperator(a, cols);
		} else if (node instanceof SelectionNode)
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
			op = new UnionOperator(a, b);
		else
			;// Joins
		return op.execute();
	}

	private void optimizeTree(PlanNode tree) {

	}

}
