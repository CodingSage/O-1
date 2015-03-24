package edu.buffalo.cse562.query;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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
import edu.buffalo.cse562.Constants;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.CartesianProduct;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;

public class SelectEvaluator implements SelectVisitor, FromItemVisitor,
		SelectItemVisitor {

	Table result, aggregatables;
	List<Table> tables;

	// List<String> columnNames;

	public SelectEvaluator() {
		tables = new ArrayList<Table>();
		// columnNames = new ArrayList<String>();
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

		PreEvaluator preEval = new PreEvaluator(tables);
		Expression w = select.getWhere();
		if (w != null) {
			w.accept(preEval);
		}
		if (preEval.getResult() != null)
			tables = preEval.getResult();

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
		extractAlias(result.getName(), arg.getAlias());
		for (Table table : tables)
			names.add(table.getName());
		Expression exp = arg.getExpression();
		ExpressionEvaluator eval = new ExpressionEvaluator(result, names);
		exp.accept(eval);
		if (exp instanceof Function) {
			String alias = arg.getAlias() != null ? arg.getAlias()
					+ Constants.AGGREGATE_INDICATOR : "";
			aggregatables.addTableColumn(eval.getOperand());
			if (eval.issum())
				aggregatables.getSchema().addColumn(
						alias + Constants.AGGREGATE_SUM, "double");
			else if (eval.iscnt())
				aggregatables.getSchema().addColumn(
						alias + Constants.AGGREGATE_COUNT, "int");
			else
				aggregatables.getSchema().addColumn(
						alias + Constants.AGGREGATE_AVG, "double");
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

		if (ordByColList != null && ordByColList.size() > 0) {
			calculateorderby(ordByColList);

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
		aggregate();
	}

	public void aggregate() {
		int columnId1 = 0;
		List<Tuple> rsResultRows = new ArrayList<Tuple>();
		int ansc = 0;
		int avgcnt = 0;
		double anss = 0;
		double avgs = 0;
		String colVal1 = null, colVal2 = null;
		List<String> columns = result.getSchema().getColName();

		for (int i = 0; i < result.getRows().size(); i++) {

			int j = i + 1;

			while (j < result.getRows().size()) {
				boolean gp = true;

				for (String col : columns) {
					columnId1 = result.getSchema().getColIndex(col);
					if (!col.contains(Constants.AGGREGATE_SUM)
							&& !col.contains(Constants.AGGREGATE_AVG)
							&& !col.contains(Constants.AGGREGATE_COUNT)) {
						colVal1 = result.getRows().get(i).getTupleValue()
								.get(columnId1);
						colVal2 = result.getRows().get(j).getTupleValue()
								.get(columnId1);

						if (!colVal1.equals(colVal2)) {
							gp = false;
							break;
						}
					}
				}

				if (!gp) {
					j--;
					break;
				}

				j++;
			}

			if (j == result.getRows().size())
				j--;

			List<String> lstCols = new ArrayList<String>();

			int l = 0;
			for (String col : columns) {
				columnId1 = result.getSchema().getColIndex(col);
				if (col.contains(Constants.AGGREGATE_COUNT)) {
					ansc = 0;
					for (int k = i; k <= j; k++)
						ansc += 1;

					lstCols.add(String.valueOf(ansc));
				} else if (col.contains(Constants.AGGREGATE_SUM)) {
					anss = 0;

					for (int k = i; k <= j; k++)
						anss += Double.valueOf(result.getRows().get(k)
								.getTupleValue().get(l));

					lstCols.add(String.valueOf(anss));
				} else if (col.contains(Constants.AGGREGATE_AVG)) {
					avgcnt = 0;
					avgs = 0;

					for (int k = i; k <= j; k++) {
						avgcnt += 1;
						avgs += Double.valueOf(result.getRows().get(k)
								.getTupleValue().get(l));
					}

					double ans = avgs / avgcnt;
					lstCols.add(String.valueOf(ans));
				} else {
					colVal1 = result.getRows().get(i).getTupleValue()
							.get(columnId1);
					lstCols.add(colVal1);
				}

				l++;

			}

			Tuple res = new Tuple(lstCols);
			rsResultRows.add(res);
			i = j;
		}

		result.setRows(rsResultRows);

	}

	public void calculateorderby(List orderbyparameters) {
		List<String> colList = new ArrayList<String>();

		for (Object col : orderbyparameters)
			colList.add((col.toString()));

		HashMap<List<String>, Tuple> h = new HashMap<List<String>, Tuple>();
		int siz = result.getRows().size();
		List<Tuple> rsResultRowsGb = new ArrayList<Tuple>();

		int columnId = 0;

		HashMap<Integer, Integer> isdesc = new HashMap<Integer, Integer>();

		for (int i = 0; i < result.getRows().size(); i++) {
			List<String> colVal = new ArrayList<String>();

			int j = 0;
			for (String column : colList) {
				String tcolumn = new String(column);
				String[] dsplit = tcolumn.split(" ");
				if (dsplit.length > 1)
					column = dsplit[0];
				columnId = result.getSchema().getColIndex(column);
				if (dsplit.length > 1 && dsplit[1].equals("DESC"))
					isdesc.put(j, 1);
				colVal.add(result.getRows().get(i).getTupleValue()
						.get(columnId));

				j++;
			}

			colVal.add(Integer.toString(i));

			h.put(colVal, result.getRows().get(i));

		}

		TreeMap<List<String>, Tuple> test = new TreeMap<List<String>, Tuple>(
				new ValueComparator(isdesc));

		test.putAll(h);

		Iterator it = test.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<List<String>, Tuple> Pair = (Entry<List<String>, Tuple>) it
					.next();
			rsResultRowsGb.add(Pair.getValue());
			it.remove();
		}
		result.setRows(rsResultRowsGb);
		return;
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

	class ValueComparator implements Comparator<List<String>> {

		Map<Integer, Integer> base;

		public ValueComparator(Map<Integer, Integer> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with
		// equals.
		public int compare(List<String> a, List<String> b) {
			int x = a.size(), y = b.size();

			for (int i = 0; i < x; i++) {
				double v1, v2;
				Date d1 = new Date();
				Date d2 = new Date();

				String[] aa = a.get(i).split("-");
				String[] bb = b.get(i).split("-");

				if (aa.length == 3 && bb.length == 3) {
					d1.setYear(Integer.valueOf(aa[0]));
					d1.setMonth(Integer.valueOf(aa[1]));
					d1.setDate(Integer.valueOf(aa[2]));

					d2.setYear(Integer.valueOf(bb[0]));
					d2.setMonth(Integer.valueOf(bb[1]));
					d2.setDate(Integer.valueOf(bb[2]));

					if (base.containsKey(i)) {
						if (d1.compareTo(d2) < 0)
							return 1;
						else if (d1.compareTo(d2) > 0)
							return -1;
					}

					if (d1.compareTo(d2) > 0)
						return 1;
					if (d1.compareTo(d2) < 0)
						return -1;
				}
				if (!a.get(i).contains("-") && a.get(i).charAt(0) >= '0'
						&& a.get(i).charAt(0) <= '9') {
					v1 = Double.parseDouble(a.get(i));
					v2 = Double.parseDouble(b.get(i));

					if (base.containsKey(i)) {
						if (v1 < v2)
							return 1;
						else if (v1 > v2)
							return -1;
					}

					if (v1 > v2)
						return 1;
					else if (v1 < v2)
						return -1;
				} else {

					if (base.containsKey(i)) {
						if (a.get(i).compareTo(b.get(i)) < 0)
							return 1;
						else if (a.get(i).compareTo(b.get(i)) > 0)
							return -1;

					}
					if (a.get(i).compareTo(b.get(i)) > 0)
						return 1;
					else if (a.get(i).compareTo(b.get(i)) < 0)
						return -1;
				}
			}
			return -1;
			// returning 0 would merge keys
		}
	}

}
