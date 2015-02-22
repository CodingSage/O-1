package edu.buffalo.cse562.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
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

	Table result, aggregatables;
	List<Table> tables;
	List<String> columnNames;

	public SelectEvaluator() {
		tables = new ArrayList<Table>();
		columnNames = new ArrayList<String>();
		aggregatables = new Table();
	}

	public Table getResult() {
		return result;
	}

	@Override
	public void visit(PlainSelect select) {
		SelectEvaluator a = new SelectEvaluator();
		FromItem item = select.getFromItem();
		item.accept(a);
		tables.add(a.getResult());
		if (select.getJoins() != null) {
			for (Object join : select.getJoins()) {
				SelectEvaluator ev = new SelectEvaluator();
				Join j = (Join) join;
				j.getRightItem().accept(ev);
				tables.add(ev.getResult());
			}
		}
		CartesianProduct prod = new CartesianProduct(tables);
		result = new Table();
		result.setRows(prod.getOutput());
		result.setSchema(prod.getResSchema());
		List<String> names = new ArrayList<String>();
		for (Table table : tables)
			names.add(table.getName());

		ExpressionEvaluator eval = new ExpressionEvaluator(result, names);
		Expression where = select.getWhere();
		if (where != null) {
			where.accept(eval);
		}

		// SelectEvaluator opProjection = new SelectEvaluator();
		/* Get the group by columns */
		List gbSelect = new ArrayList<PlainSelect>();
		gbSelect = select.getGroupByColumnReferences();

		/* Get the order by columns */
		List orderbyList = new ArrayList<PlainSelect>();
		orderbyList = select.getOrderByElements();

		for (Object sitem : select.getSelectItems()) {
			((SelectItem) sitem).accept(this);
		}
		result = aggregatables;
		setGroupByonResults(gbSelect, orderbyList);
		setFinalProjection();
		// Set the projected columns to the new relation - this relation is the
		// final output
	}

	@Override
	public void visit(Union arg) {
		Table union = new Table();
		for (Object select : arg.getPlainSelects()) {
			SelectEvaluator s = new SelectEvaluator();
			((PlainSelect) select).accept(s);
			union.append(s.getResult());
		}
		result = union;
	}

	@Override
	public void visit(net.sf.jsqlparser.schema.Table table) {
		result = DataManager.getInstance().getTable(table.getName());
	}

	@Override
	public void visit(SubSelect arg) {
		SelectEvaluator eval = new SelectEvaluator();
		arg.getSelectBody().accept(eval);
		result = eval.getResult();
		// TODO check name is present
		String origName = result.getName();
		String alias = arg.getAlias();
		extractAlias(origName, alias);
	}

	private void extractAlias(String origName, String alias) {
		if (alias != null && !alias.equals("")) {
			if (origName == null) {
				
			} else {
				Schema s = result.getSchema();
				List<String> colNames = s.getColName();
				for (String str : colNames) {
					str.replaceAll(origName, alias);
				}
				s.setColName(colNames);
				result.setSchema(s);
			}
		}
	}

	@Override
	public void visit(SubJoin arg) {
		return;
	}

	@Override
	public void visit(AllColumns arg) {
		aggregatables = result;
		return;
	}

	@Override
	public void visit(AllTableColumns arg) {
		return;
	}

	@Override
	public void visit(SelectExpressionItem arg) {
		// TODO evaluate expression
		List<String> names = new ArrayList<String>();
		// extractAlias(result.getName(), arg.getAlias());
		for (Table table : tables)
			names.add(table.getName());
		Expression exp = arg.getExpression();
		ExpressionEvaluator eval = new ExpressionEvaluator(result, names);
		exp.accept(eval);
		if (exp instanceof Function) {
			aggregatables.addTableColumn(eval.getOperand());
			if (eval.issum())
				aggregatables.getSchema().addColumn("Sum", "double");
			else if (eval.iscnt())
				aggregatables.getSchema().addColumn("Count", "int");
			else
				aggregatables.getSchema().addColumn("Average", "double");
		} else {
			aggregatables.addTableColumn(eval.getOperand());
			List<String> cols = eval.getResult();
			if (arg.getAlias() != null)
				aggregatables.getSchema().addColumn(arg.getAlias(), "string");
			else
				for (String col : cols) {
					aggregatables.getSchema().addColumn(col, "string");
				}
		}
	}

	/* The method does a group by on the records */
	public void setGroupByonResults(List gbColList, List ordByColList) {
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
					columnId = result.getSchema().getColIndex(column);
					colVal1.add(result.getRows().get(i).getTupleValue()
							.get(columnId));
				}
				if (h.containsKey(colVal1))
					continue;
				h.put(colVal1, 1);
				// add i to result
				curRow = new Tuple();
				for (String col : result.getSchema().getColName()) {
					int j = result.getSchema().getColIndex(col);
					curRow.insertColumn(result.getValue(i, j));
				}
				rsResultRowsGb.add(curRow);
				for (int j = i + 1; j < result.getRows().size(); j++) {
					// Tuple resRow = new Tuple();
					List<String> colVal2 = new ArrayList<String>();
					for (String column : colList) {
						columnId = result.getSchema().getColIndex(column);
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
						curRow = new Tuple();
						for (String col : result.getSchema().getColName()) {
							int a = result.getSchema().getColIndex(col);
							curRow.insertColumn(result.getValue(j, a));
						}
						rsResultRowsGb.add(curRow);
					}
				}
			}
			result.setRows(rsResultRowsGb);
		}
	}

	/* Set the final projection based on the select items */
	public void setFinalProjection() {
		Table rsResult = new Table();
		int columnId1 = 0;
		String colVal = null;
		List<Tuple> rsResultRows = new ArrayList<Tuple>();
		if (result.getRows() != null) {
			for (int i = 0; i < result.getRows().size(); i++) {
				Tuple resRow = new Tuple();
				int aggcnt = 0, j = 0;
				Schema resSchema = result.getSchema();
				for (String column : resSchema.getColName()) {
					columnId1 = result.getSchema().getColIndex(column);
					if (columnId1 != -1) {
						colVal = result.getRows().get(i).getTupleValue().get(j);
						resRow.insertColumn(colVal);
						j++;
					} else {
						resRow.insertColumn(aggregatables.getValue(i, aggcnt));
						aggcnt++;
					}

				}
				rsResultRows.add(resRow);
			}
			rsResult.setRows(rsResultRows);
		}
		result.setRows(rsResultRows);
		calculateAggr1();
	}

	public void calculateAggr() {
		int i;
		List<Integer> aggr = new ArrayList<Integer>(); // aggr indexes
		for (i = 0; i < columnNames.size(); i++) {
			if (!(columnNames.get(i).equals("Average")
					|| columnNames.get(i).equals("Sum") || columnNames.get(i)
					.equals("Count")))
				i++;
			else
				aggr.add(i);
		}
		Tuple t = new Tuple(columnNames.size());
		List<Tuple> tuples = new ArrayList<Tuple>();
		for (Tuple row : result.getRows()) {
			boolean same = true;
			int count = 0;
			for (int j = 0; j <= i; j++)
				// TODO check group by indexes
				if (!row.getValue(j).equals(t.getValue(j))) {
					same = false;
					break;
				}
			if (same) {
				count++;
				for (int a = 0; a < aggr.size(); a++) {
					double v = Double.parseDouble(t.getValue(aggr.get(a)));
					v += Double.parseDouble(row.getValue(aggr.get(a)));
					t.setValue(aggr.get(a), v + "");
				}
			} else {
				boolean init = false;
				for (int a = 0; a < t.getTupleValue().size(); a++) {
					if (t.getValue(a).isEmpty())
						init = true;
				}
				if (init) {
					for (int j = 0; j < row.getValues().size(); j++)
						t.setValue(j, row.getValue(j));
				} else {
					for (int a = 0; a < aggr.size(); a++) {
						if (columnNames.get(aggr.get(a)).equals("Average")) {
							double avg = Double.parseDouble(t.getValue(aggr
									.get(a))) / count;
							t.setValue(aggr.get(a), avg + "");
						} else if (columnNames.get(aggr.get(a)).equals("Count"))
							t.setValue(aggr.get(a), count + "");
					}
					tuples.add(t);
					for (int j = 0; j < row.getValues().size(); j++)
						t.setValue(j, row.getValue(j));
				}
				count = 0;
			}
		}
		result.setRows(tuples);
	}

	public void calculateAggr1() {
		int columnId1 = 0;
		List<Tuple> rsResultRows = new ArrayList<Tuple>();
		int ansc = 1, avgcnt = 0, prev = 0, cntindex = -1, sumIndex = -1;
		Double anss = (double) 0, avgs = (double) 0;
		boolean group = true;
		String colVal1 = null, colVal2 = null;
		List<String> columns = result.getSchema().getColName();

		for (int i = 1; i < result.getRows().size(); i++) {
			group = true;
			int j = 0;
			List<String> lstCols = new ArrayList<String>();

			HashMap<Integer, Integer> h = new HashMap<Integer, Integer>();

			for (String col : columns) {
				columnId1 = result.getSchema().getColIndex(col);
				if (col.equals("Count"))
					h.put(j, 0);
				if (col.equals("Sum"))
					h.put(j, 1);
				if (col.equals("Average"))
					h.put(j, 2);

				if (!col.equals("Sum") && !col.equals("Average")
						&& !col.equals("Count")) {
					colVal1 = result.getRows().get(prev).getTupleValue()
							.get(columnId1);
					colVal2 = result.getRows().get(i).getTupleValue()
							.get(columnId1);
					lstCols.add(colVal1);
					if (!colVal1.equals(colVal2)) {
						group = false;
					}
				}
				j++;
			}

			if (h.size() == 0) {
				rsResultRows.add(result.getRows().get(i));
				continue;
			}

			int colIndex = -1;
			int oper = -1;
			boolean addFlag = false;
			Tuple res = new Tuple();
			for (Map.Entry<Integer, Integer> entry : h.entrySet()) 
			{

				colIndex = entry.getKey();
				oper = entry.getValue();

				if (group) 
				{

					if (oper == 0)
						ansc += 1;
					if (oper == 1)
						anss += Double.valueOf(result.getRows().get(i)
								.getTupleValue().get(colIndex));
					if (oper == 2) 
					{
						avgcnt += 1;
						avgs += Double.valueOf(result.getRows().get(i)
								.getTupleValue().get(colIndex));

					}
				}
				else 
				{

					prev = i;
					addFlag = true;
						
					if (oper == 0)
					{
						lstCols.add(String.valueOf(ansc));
						ansc = 1;
					}
					if (oper == 1) {
						lstCols.add(String.valueOf(anss));
						anss = Double.valueOf(result.getRows().get(prev)
								.getTupleValue().get(colIndex));
					}
					if (oper == 2) {
						double avg = avgs / avgcnt;
						lstCols.add(String.valueOf(avg));
						avgs = Double.valueOf(result.getRows().get(prev)
								.getTupleValue().get(colIndex));
						avgcnt = 1;
					}
				
				}
			}
			
			if (addFlag) 
			{
				res = new Tuple(lstCols);
				rsResultRows.add(res);
				
				if(group == false && i == result.getRows().size() - 1)
				{
					List<String> tlstCols = new ArrayList<String>();
			
					for (String col : columns)
					{
						columnId1 = result.getSchema().getColIndex(col);
						colVal2 = result.getRows().get(prev).getTupleValue().get(columnId1);
						
						if(!col.equals("Sum") & !col.equals("Average") && !col.equals("Count"))
							tlstCols.add(colVal2);
					}
					
					tlstCols.add(String.valueOf(ansc));
				
					rsResultRows.add(new Tuple(tlstCols));
				}

			}
		}
		result.setRows(rsResultRows);
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
