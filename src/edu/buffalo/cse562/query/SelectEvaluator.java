package edu.buffalo.cse562.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
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
		/* Get the group by columns */
		List<PlainSelect> gbSelect = new ArrayList<PlainSelect>();
		gbSelect = select.getGroupByColumnReferences();

		/* Get the order by columns */
		List<PlainSelect> orderbyList = new ArrayList<PlainSelect>();
		orderbyList = select.getOrderByElements();

		boolean isCount = false, isSum = false, isAvg = false;

		for (Object sitem : select.getSelectItems()) {
			if (sitem.toString().contains("COUNT"))
				isCount = true;
			else if (sitem.toString().contains("SUM"))
				isSum = true;
			else if (sitem.toString().contains("AVG"))
				isAvg = true;
			else
				((SelectItem) sitem).accept(this);
		}
		setFinalProjection(gbSelect, orderbyList, isCount, isSum, isAvg);
		
		// Set the projected columns to the new relation - this relation is the
		// final output
	}

	@Override
	public void visit(Union arg) {
		Table union = new Table();
		for(Object select : arg.getPlainSelects()){
			SelectEvaluator s = new SelectEvaluator();
			((PlainSelect)select).accept(s);
			union.append(s.getResult());
		}
		result = union;
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
	public void setFinalProjection(List<PlainSelect> gbColList,
			List<PlainSelect> ordByColList, boolean isCount, boolean isSum,
			boolean isAvg) {
		if (columnNames.size() == 0)
			return;
		/* If there is a group by list then we need to sort */
		if (gbColList != null && gbColList.size() != 0) {
			List<String> colList = new ArrayList<String>();

			for (Object col : gbColList)
				colList.add((col.toString()));
			// Loop through each row of the record

			int columnId = 0;

			HashMap<List<String>, Integer> h = new HashMap<List<String>, Integer>();

			List<Tuple> rsResultRowsGb = new ArrayList<Tuple>();

			for (int i = 0; i < result.getRows().size(); i++) {
				Tuple curRow = new Tuple();
				List<String> colVal1 = new ArrayList<String>();
				for (String column : colList) {
					columnId = resultSchema.getColIndex(column);
					colVal1.add(result.getRows().get(i).getTupleValue()
							.get(columnId));
				}

				if (h.containsKey(colVal1))
					continue;

				h.put(colVal1, 1);
				// add i to result
				curRow = new Tuple(colVal1);

				rsResultRowsGb.add(curRow);

				for (int j = i + 1; j < result.getRows().size(); j++) {
					// Tuple resRow = new Tuple();
					List<String> colVal2 = new ArrayList<String>();

					for (String column : colList) {
						columnId = resultSchema.getColIndex(column);
						colVal2.add(result.getRows().get(j).getTupleValue()
								.get(columnId));
					}

					int found = 0;

					for (int k = 0; k < colVal1.size(); k++) {
						if (!colVal1.get(k).equals(colVal2.get(k))) {
							found = 1; // yaay, there's a mismatch. less work :)
							break;
						}
					}

					if (found == 0) {
						// add j to result
						curRow = new Tuple(colVal2);
						int count = h.get(colVal2);
						count += 1;
						h.put(colVal2, count);
						rsResultRowsGb.add(curRow);
					}

				}

			}
			// result.setRows(rsResultRowsGb);
			setRsltGrpBy(h);

		} else {
			Table rsResult = new Table();
			int columnId1 = 0;
			String colVal = null;
			List<Tuple> rsResultRows = new ArrayList<Tuple>();
			if (result.getRows() != null) {
				for (int i = 0; i < result.getRows().size(); i++) {
					Tuple resRow = new Tuple();
					for (String column : columnNames) {
						columnId1 = resultSchema.getColIndex(column);
						colVal = result.getRows().get(i).getTupleValue()
								.get(columnId1);
						resRow.insertColumn(colVal);
					}
					rsResultRows.add(resRow);
				}
				rsResult.setRows(rsResultRows);
			}
			result.setRows(rsResultRows);
		}
	}

	public void setRsltGrpBy(HashMap<List<String>, Integer> hmResults) {

		List<Tuple> rsResults = new ArrayList<Tuple>();
		for (List<String> key : hmResults.keySet()) {
			Integer value = hmResults.get(key);
			key.add(String.valueOf(value));
			Tuple newRow = new Tuple(key);
			rsResults.add(newRow);
		}
		result.setRows(rsResults);
	}
}
