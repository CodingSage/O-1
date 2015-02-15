package edu.buffalo.cse562.query;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.CartesianProduct;
import edu.buffalo.cse562.model.Table;

public class SelectEvaluator implements SelectVisitor, FromItemVisitor,
		SelectItemVisitor {

	Table result;
	List<Table> tables;
	List<String> columnNames;

	public SelectEvaluator() {
		tables = new ArrayList<Table>();
		columnNames = new ArrayList<String>();
	}

	public Table getResult() {
		return result;
	}

	@Override
	public void visit(PlainSelect select) {
		FromItem item = select.getFromItem();
		item.accept(this);

		for (Object join : select.getJoins()) {
			Join j = (Join) join;
			j.getRightItem().accept(this);
		}

		CartesianProduct prod = new CartesianProduct(tables);
		result = new Table();
		result.setRows(prod.getOutput());
		for (Object sitem : select.getSelectItems()) {
			((SelectItem) sitem).accept(this);
		}
		ExpressionEvaluator eval = new ExpressionEvaluator();
		Expression where = select.getWhere();
		if (where != null)
			where.accept(eval);
	}

	@Override
	public void visit(Union arg) {
		System.out.println("union");
	}

	@Override
	public void visit(net.sf.jsqlparser.schema.Table table) {
		tables.add(DataManager.getInstance().getTable(table.getName()));
	}

	@Override
	public void visit(SubSelect arg) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SubJoin arg) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(AllColumns arg) {
		return;
	}

	@Override
	public void visit(AllTableColumns arg) {
		// TODO
	}

	@Override
	public void visit(SelectExpressionItem arg) {
		// TODO evaluate expression
		Expression exp = arg.getExpression();
		ExpressionEvaluator eval = new ExpressionEvaluator();
		exp.accept(eval);
		columnNames.addAll(eval.getResult());
	}

}
