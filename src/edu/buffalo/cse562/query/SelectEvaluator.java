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
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;

public class SelectEvaluator implements SelectVisitor, FromItemVisitor,
		SelectItemVisitor {

	Table result;
	List<Table> tables;
	List<String> columnNames;
	Schema resultSchema;

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
		if (select.getJoins() != null) {
			for (Object join : select.getJoins()) {
				Join j = (Join) join;
				j.getRightItem().accept(this);
			}
		}
		CartesianProduct prod = new CartesianProduct(tables);
		result = new Table();
		result.setRows(prod.getOutput());
		resultSchema = new Schema();
		resultSchema = prod.getResSchema();
		List<String> names = new ArrayList<String>();
		for (Table table : tables)
			names.add(table.getName());
		ExpressionEvaluator eval = new ExpressionEvaluator(result, names);
		Expression where = select.getWhere();
		if (where != null)
			where.accept(eval);
		// SelectEvaluator opProjection = new SelectEvaluator();
		for (Object sitem : select.getSelectItems()) {
			((SelectItem) sitem).accept(this);
		}
		setFinalProjection();
		// Set the projected columns to the new relation - this relation is the
		// final output
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

	/* Set the final projection based on the select items */
	public void setFinalProjection() {
		if (columnNames.size() == 0)
			return;
		Table rsResult = new Table();
		int columnId = 0;
		String colVal = null;
		List<Tuple> rsResultRows = new ArrayList<Tuple>();
		if (result.getRows() != null) {
			for (int i = 0; i < result.getRows().size(); i++) {
				Tuple resRow = new Tuple();
				for (String column : columnNames) {
					columnId = resultSchema.getColIndex(column);
		   			colVal = result.getRows().get(i).getTupleValue().get(columnId);
		   			resRow.insertColumn(colVal);
				}
				rsResultRows.add(resRow);
			}
			rsResult.setRows(rsResultRows);
		}
		result.setRows(rsResultRows);
	}
}
