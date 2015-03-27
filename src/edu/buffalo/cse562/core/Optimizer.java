package edu.buffalo.cse562.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.PlanNode;
import edu.buffalo.cse562.checkpoint1.ProductNode;
import edu.buffalo.cse562.checkpoint1.SelectionNode;
import edu.buffalo.cse562.checkpoint1.TableScanNode;
import edu.buffalo.cse562.checkpoint1.PlanNode.Binary;
import edu.buffalo.cse562.checkpoint1.PlanNode.Unary;
import edu.buffalo.cse562.model.JoinNode;

public class Optimizer {
	
	public static void optimizeTree(PlanNode node, PlanNode parent, Set<Expression> exprs, Set<Expression> remove) {
		if (node instanceof PlanNode.Unary) {
			List<Expression> exps = null;
			if (node instanceof SelectionNode) {
				exps = extractExpression(((SelectionNode) node).getCondition());
				exprs.addAll(exps);
			}
			optimizeTree(((PlanNode.Unary) node).getChild(), node, exprs, remove);
			if(!remove.isEmpty() && exps != null){
				List<Expression> exp = formatExpression(remove, exps);
				if(exp.isEmpty())
					removeSelectNode((SelectionNode) node, parent);
				else
					((SelectionNode)node).setCondition(exp);
			}
		} else if (node instanceof PlanNode.Binary) {
			PlanNode.Binary bnode = (PlanNode.Binary) node;
			optimizeTree(bnode.getLHS(), node, exprs, remove);
			optimizeTree(bnode.getRHS(), node, exprs, remove);
			if (node instanceof ProductNode) {
				PlanNode l = ((ProductNode) node).getLHS();
				PlanNode r = ((ProductNode) node).getRHS();
				List<Column> le = l.getSchemaVars();
				List<Column> re = r.getSchemaVars();
				Iterator<Expression> it = exprs.iterator();
				while(it.hasNext()) {
					Expression exp = it.next();
					Expression lexp = ((BinaryExpression) exp).getLeftExpression();
					Expression rexp = ((BinaryExpression) exp).getRightExpression();
					if (lexp instanceof Column && rexp instanceof Column) {
						String rname = ((Column)rexp).getWholeColumnName();
						String lname = ((Column)lexp).getWholeColumnName();
						if ((isColumnPresent(le, lname) && isColumnPresent(re, rname))
							|| (isColumnPresent(re, lname) && isColumnPresent(le, rname))){
							JoinNode join = new JoinNode((BinaryExpression)exp);
							addJoin(parent, (Binary) node, join);
							it.remove();
							remove.add(exp);
							break;
						}
					}
				}
			}
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
					if (rhs instanceof Column)
						if (!isColumnPresent(table,
								((Column) rhs).getWholeColumnName()))
							continue;
					if (lhs instanceof Column)
						if (!isColumnPresent(table,
								((Column) lhs).getWholeColumnName()))
							continue;
					PlanNode.Unary a = new SelectionNode(exp);
					addNode(parent, node, a);
					remove.add(exp);
					i.remove();
				}
			}
		}
	}

	private static void removeSelectNode(SelectionNode node, PlanNode parent) {
		if(parent instanceof Unary)
			((Unary) parent).setChild(node.getChild());
		else{
			Binary bnode = (Binary)parent;
			if(bnode.getLHS() == node)
				bnode.setLHS(node.getChild());
			else
				bnode.setRHS(node.getChild());
		}
	}

	private static List<Expression> formatExpression(Set<Expression> remove, List<Expression> exps) {
		for(Expression exp : remove){
			if(exps.contains(exp))
				exps.remove(exp);
		}
		return exps;
	}

	private static boolean isColumnPresent(List<Column> cols, String colName) {
		//if(cols == null)
			//return false;
		for (Column col : cols)
			if (col.getWholeColumnName().equals(colName))
				return true;
		return false;
	}

	private static void addJoin(PlanNode parent, Binary node, JoinNode join) {
		if (parent instanceof Unary)
			((Unary) parent).setChild(join);
		else {
			Binary bnode = (Binary) parent;
			if (bnode.getLHS() == node)
				bnode.setLHS(join);
			else
				bnode.setRHS(join);
		}
		join.setLHS(node.getLHS());
		join.setRHS(node.getRHS());
	}

	private static void addNode(PlanNode parent, PlanNode node, Unary a) {
		a.setChild(node);
		if (parent == null)
			return;
		if (parent instanceof PlanNode.Unary) {
			((PlanNode.Unary) parent).setChild(a);
		} else {
			PlanNode.Binary binary = (PlanNode.Binary) parent;
			if (binary.getLHS() == node)
				binary.setLHS(a);
			else
				binary.setRHS(a);
		}
	}

	private static List<Expression> extractExpression(Expression exp) {
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

}
