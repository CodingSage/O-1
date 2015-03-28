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
import edu.buffalo.cse562.model.JoinNode;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.operators.AggregateOperator;
import edu.buffalo.cse562.query.operators.GroupByOperator;
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
		Optimizer.optimizeTree(raTree, null, new HashSet<Expression>(),
				new HashSet<Expression>());
		System.out.println(raTree);
		System.out.println("---------------------------------");
		Table result = evaluateTree(raTree);
		if (result != null)
		{
			for(int i=0;i<result.getRows().size();i++)
					System.out.println(result.getRows().get(i));
			
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
					((TableScanNode) tree).table.getWholeTableName());
			res.loadData();
		}
		return res;
	}

	private Table evaluate(PlanNode.Unary node, Table a) {
		Operator op = null;
		String rangeVariable = "";
		if (node instanceof LimitNode)
			op = new LimitOperator(((LimitNode) node).getCount(), a);
		else if (node instanceof ProjectionNode) {
			ProjectionNode pnode = (ProjectionNode) node;
			List<Target> cols = pnode.getColumns();
			op = new ProjectionOperator(a, cols);
			net.sf.jsqlparser.schema.Table range = pnode.getRangeVariable();
			if (range != null)
				rangeVariable = range.getWholeTableName();
		} else if (node instanceof SelectionNode)
			op = new SelectOperator(a, ((SelectionNode) node).getCondition());
		else if (node instanceof SortNode) {
			SortNode sort = (SortNode) node;
			List<Ordering> order = sort.getSorts();
			op = new OrderByOperator(a, order);
		}
		else {
			AggregateNode agg = (AggregateNode) node;
			List<Target> groups = agg.getGroupByVars();
			List<AggColumn> aggColumns = agg.getAggregates();
			if (groups != null && !groups.isEmpty()) {
				op = new GroupByOperator(a, groups, aggColumns);
				a = op.execute();
				net.sf.jsqlparser.schema.Table range = agg.getRangeVariable();
				if (range != null)
					rangeVariable = range.getWholeTableName();
				
			}
			op = new AggregateOperator(groups, aggColumns, a);
		}
		Table res = op.execute();
		if (!rangeVariable.isEmpty())
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
			if(node instanceof JoinNode)
			   op = new JoinOperator(a,b,((JoinNode) node).exp);	;// Joins  
				System.out.println("CHE");
		return op.execute();
	}

}
