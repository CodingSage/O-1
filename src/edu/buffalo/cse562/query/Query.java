package edu.buffalo.cse562.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.AggregateNode;
import edu.buffalo.cse562.checkpoint1.AggregateNode.AggColumn;
import edu.buffalo.cse562.checkpoint1.LimitNode;
import edu.buffalo.cse562.checkpoint1.PlanNode;
import edu.buffalo.cse562.checkpoint1.PlanNode.Unary;
import edu.buffalo.cse562.checkpoint1.ProductNode;
import edu.buffalo.cse562.checkpoint1.ProjectionNode;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.checkpoint1.SelectionNode;
import edu.buffalo.cse562.checkpoint1.SortNode;
import edu.buffalo.cse562.checkpoint1.SortNode.Ordering;
import edu.buffalo.cse562.checkpoint1.TableScanNode;
import edu.buffalo.cse562.checkpoint1.UnionNode;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.operators.AggregateOperator;
import edu.buffalo.cse562.query.operators.GroupByOperator;
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
		optimizeTree(raTree, null, new HashSet<Expression>());
		System.out.println(raTree);
		System.out.println("---------------------------------");
		Table result = evaluateTree(raTree);
		if (result != null)
			System.out.print(result.toString());
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
			if(range != null)
				rangeVariable = range.getWholeTableName();
		} else if (node instanceof SelectionNode)
			op = new SelectOperator(a, ((SelectionNode) node).getCondition());
		else if (node instanceof SortNode) {
			SortNode sort = (SortNode) node;
			List<Ordering> order = sort.getSorts();
			op = new OrderByOperator(a, order);
		} else {
			AggregateNode agg = (AggregateNode) node;
			List<Target> groups = agg.getGroupByVars();
			List<AggColumn> aggColumns = agg.getAggregates();
			if (groups != null && !groups.isEmpty()) {
				op = new GroupByOperator(a, groups, aggColumns);
				a = op.execute();
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
			;// Joins
		return op.execute();
	}

	private List<Column> optimizeTree(PlanNode node, PlanNode parent, Set<Expression> exprs) {
		if (node instanceof PlanNode.Unary) {
			if (node instanceof SelectionNode) {
				List<Expression> exps = extractExpression(((SelectionNode) node).getCondition());
				exprs.addAll(exps);
			}
			return optimizeTree(((PlanNode.Unary) node).getChild(), node, exprs);
		} else if (node instanceof PlanNode.Binary) {
			PlanNode.Binary bnode = (PlanNode.Binary) node;
			List<Column> lc = optimizeTree(bnode.getLHS(), node, exprs);
			List<Column> rc = optimizeTree(bnode.getRHS(), node, exprs);
			if (node instanceof ProductNode) {
				// TODO check join addition
				PlanNode l = ((ProductNode) node).getLHS();
				PlanNode r = ((ProductNode) node).getRHS();
				// l.getsc
			}
			lc.addAll(rc);
			return lc;
		} else {
			List<Column> table = ((TableScanNode) node).getSchemaVars();
			Iterator<Expression> i = exprs.iterator();
			while (i.hasNext()) {
				Expression exp = i.next();
				if (exp instanceof BinaryExpression) {
					Expression lhs = ((BinaryExpression) exp)
							.getLeftExpression();
					Expression rhs = ((BinaryExpression) exp)
							.getRightExpression();
					if ((rhs instanceof Column && table.contains(((Column) rhs)
							.getWholeColumnName()))
							|| (lhs instanceof Column && table
									.contains(((Column) lhs)
											.getWholeColumnName()))) {
						PlanNode.Unary a = new SelectionNode(exp);
						addNode(parent, node, a);
						i.remove();
					}
				}
			}
			return table;
		}
	}

	private List<Expression> extractExpression(Expression exp) {
		List<Expression> e = new ArrayList<Expression>();
		if (exp instanceof AndExpression) {
			e.addAll(extractExpression(((AndExpression) exp)
					.getLeftExpression()));
			e.addAll(extractExpression(((AndExpression) exp)
					.getRightExpression()));
		} else {
			e.add(exp);
		}
		return e;
	}

	private void addNode(PlanNode parent, PlanNode node, Unary a) {
		a.setChild(node);
		if (parent == null)
			return;
		if (parent instanceof PlanNode.Unary) {
			((PlanNode.Unary) parent).setChild(a);
		} else {
			PlanNode.Binary binary = (PlanNode.Binary) parent;
			a.setChild(node);
			if (binary.getLHS() == node)
				binary.setLHS(a);
			else
				binary.setRHS(a);
		}
	}
}
