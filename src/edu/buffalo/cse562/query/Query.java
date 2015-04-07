package edu.buffalo.cse562.query;

import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.checkpoint1.AggregateNode;
import edu.buffalo.cse562.checkpoint1.AggregateNode.AggColumn;
import edu.buffalo.cse562.checkpoint1.LimitNode;
import edu.buffalo.cse562.checkpoint1.PlanNode;
import edu.buffalo.cse562.checkpoint1.ProductNode;
import edu.buffalo.cse562.checkpoint1.ProjectionNode;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.checkpoint1.SelectionNode;
import edu.buffalo.cse562.checkpoint1.SortNode;
import edu.buffalo.cse562.checkpoint1.SortNode.Ordering;
import edu.buffalo.cse562.checkpoint1.TableScanNode;
import edu.buffalo.cse562.checkpoint1.UnionNode;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.core.Optimizer;
import edu.buffalo.cse562.model.FileFunction;
import edu.buffalo.cse562.model.JoinNode;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;
import edu.buffalo.cse562.query.operators.AggregateOperator;
import edu.buffalo.cse562.query.operators.JoinOperator;
import edu.buffalo.cse562.query.operators.LimitOperator;
import edu.buffalo.cse562.query.operators.OrderByOperator;
import edu.buffalo.cse562.query.operators.ProjectionOperator;
import edu.buffalo.cse562.query.operators.SelectOperator;
import edu.buffalo.cse562.query.operators.UnionOperator;

public class Query {

	private PlanNode raTree;

	public Query(PlanNode query) {
		this.raTree = query;
	}

	public void evaluate() {
		// System.out.println(raTree);
		// System.out.println("---------------------------------");
		Optimizer.optimizeTree(raTree, null, new HashSet<Expression>(),
				new HashSet<Expression>());
		System.out.println(raTree);
		System.out.println("---------------------------------");
		Table finalRes = new Table();
		Table result = evaluateTree(raTree);
		while (result != null) {
			if (!result.isEmpty())
				finalRes.append(result);
			result = evaluateTree(raTree);
		}
		if (finalRes != null) {
			for (int i = 0; i < finalRes.getRows().size(); i++)
				System.out.println(finalRes.getRows().get(i));
		}
	}

	private Table evaluateTree(PlanNode tree) {
		Table res = new Table();
		Table a, b;
		if (tree instanceof PlanNode.Unary) {
			a = evaluateTree(((PlanNode.Unary) tree).getChild());
			if (a == null)
				return null;
			if (tree instanceof AggregateNode) {
				while (a != null) {
					if(!a.isEmpty())
						evaluate((PlanNode.Unary) tree, a);
					a = evaluateTree(((PlanNode.Unary) tree).getChild());
				}
			}
			res = evaluate((PlanNode.Unary) tree, a);
		} else if (tree instanceof PlanNode.Binary) {
			PlanNode.Binary btree = (PlanNode.Binary) tree;
			a = evaluateTree(btree.getLHS());
			b = evaluateTree(btree.getRHS());
			if(btree instanceof JoinNode){
				while(a != null){
					if(!a.isEmpty())
						evaluate(btree, a, null);
					a = evaluateTree(btree.getLHS());
				}
			}
			res = evaluate(btree, null, b);
		} else {
			res = DataManager.getInstance().getTable(
					((TableScanNode) tree).table.getWholeTableName());
			a = new Table(res.getSchema());
			a.setName(res.getName());
			res = a;
			Tuple tup = FileFunction.readTable(res.getName());
			if (tup == null)
				return null;
			res.addRow(tup);
		}
		return res;
	}

	private Table evaluate(PlanNode.Unary node, Table a) {
		Operator op = null;
		String rangeVariable = "";
		if (node instanceof LimitNode) {
			if (a == null)
				return null;
			op = new LimitOperator(((LimitNode) node).getCount(), a);
		} else if (node instanceof ProjectionNode) {
			//System.out.println("Projection");
			if (a == null)
				return null;
			ProjectionNode pnode = (ProjectionNode) node;
			List<Target> cols = pnode.getColumns();
			op = new ProjectionOperator(a, cols);
			net.sf.jsqlparser.schema.Table range = pnode.getRangeVariable();
			if (range != null)
				rangeVariable = range.getWholeTableName();
		} else if (node instanceof SelectionNode) {
			//System.out.println("Select");
			if (a == null)
				return null;
			op = new SelectOperator(a, ((SelectionNode) node).getCondition());
			/*
			 * if (node.getChild() instanceof TableScanNode) op = new
			 * SelectLoadOperator(a, ((SelectionNode) node).getCondition());
			 */
		} else if (node instanceof SortNode) {
			//System.out.println("Sort");
			SortNode sort = (SortNode) node;
			List<Ordering> order = sort.getSorts();
			op = new OrderByOperator(a, order);
		} else {
			//System.out.println("Aggregate");
			AggregateNode agg = (AggregateNode) node;
			List<Target> groups = agg.getGroupByVars();
			List<AggColumn> aggColumns = agg.getAggregates();
			/*if (groups != null && !groups.isEmpty()) {
				op = new GroupByOperator(a, groups, aggColumns);
				a = op.execute();
			}*/
			net.sf.jsqlparser.schema.Table range = agg.getRangeVariable();
			if (range != null)
				rangeVariable = range.getWholeTableName();
			op = new AggregateOperator(groups, aggColumns, a);
		}
		Table res = op.execute();
		if (!rangeVariable.isEmpty() && !res.isEmpty())
			res.getSchema().changeTableName(rangeVariable);
		return res;
	}

	private Table evaluate(PlanNode.Binary node, Table a, Table b) {
		Operator op = null;
		if (node instanceof ProductNode)
			;
		else if (node instanceof UnionNode)
			op = new UnionOperator(a, b);
		else
			op = new JoinOperator(a, b, ((JoinNode) node).getCondition());
		return op.execute();
	}

}
