package edu.buffalo.cse562.query.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.plan.AggregateNode.AType;
import edu.buffalo.cse562.checkpoint1.plan.AggregateNode.AggColumn;
import edu.buffalo.cse562.checkpoint1.plan.ProjectionNode.Target;
import edu.buffalo.cse562.model.ColumnType;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;
import edu.buffalo.cse562.model.Utilities;
import edu.buffalo.cse562.query.Evaluator;

public class AggregateOperator extends Operator {

	private List<AggColumn> aggregates;
	private List<Target> groups;
	private static Map<String, Tuple> groupAgg = new HashMap<String, Tuple>();
	private static Schema schema = null;
	private static List<Integer> is = null;
	private static Tuple tuple = null;

	public AggregateOperator(List<Target> groupCols,
			List<AggColumn> aggColumns, Table a) {
		groups = groupCols;
		table = a;
		aggregates = aggColumns;
	}

	@Override
	protected Table evaluate() {
		if (table == null) {
			Table res = new Table();
			List<Integer> avgIs = new ArrayList<Integer>();

			for (int i = 0; i < aggregates.size(); i++) {
				if (aggregates.get(i).aggType == AType.AVG)
					avgIs.add(i);
			}
			for (String key : groupAgg.keySet()) {
				List<String> cols = Utilities.splitStrings('@', key);
				Tuple tup = new Tuple();
				for (int i = 1; i < cols.size(); i++) {
					ColumnType type = schema.getColType().get(i - 1);
					LeafValue val = Utilities.toLeafValue(cols.get(i), type);
					tup.insertColumn(val);
				}
				Tuple a = Tuple.merge(tup, groupAgg.get(key));
				for (Integer i : avgIs) {
					String v = ((StringValue) a.getValue(groups.size() + i))
							.getValue().toString();
					String v1 = v.substring(v.indexOf(':') + 1);
					String v2 = v.substring(0, v.indexOf(':'));
					double avg = Double.parseDouble(v1) / Integer.parseInt(v2);
					a.setValue(groups.size() + i, new DoubleValue(avg));
				}
				res.addRow(a);
			}
			groupAgg.clear();
			res.setSchema(schema);
			schema = null;
			return res;
		}

		Table res = new Table();
		if (table.isEmpty())
			return res;
		if (schema == null) {
			Schema s = table.getSchema();
			schema = new Schema();
			is = new ArrayList<Integer>();
			tuple = new Tuple();
			if (groups != null) {
				for (Target g : groups) {
					String colName = ((Column) g.expr).getWholeColumnName();
					ColumnType colType = s.getType(colName);
					schema.addColumn(colName, colType);
					int i = s.getColIndex(colName);
					is.add(i);
				}
			}
			for (AggColumn col : aggregates) {
				ColumnType type = null;
				if (col.aggType == AType.AVG || col.aggType == AType.SUM
						|| col.aggType == AType.MAX || col.aggType == AType.MIN)
					type = ColumnType.DOUBLE;
				else
					type = ColumnType.INT;
				if (col.aggType == AType.AVG)
					tuple.insertColumn(new StringValue("'0:0'"));
				else if (col.aggType == AType.MIN)
					tuple.insertColumn(new DoubleValue(Double.MAX_VALUE));
				else if (col.aggType == AType.MAX || col.aggType == AType.SUM)
					tuple.insertColumn(new DoubleValue(0));
				else
					tuple.insertColumn(new LongValue(0));
				schema.addColumn(col.name, type);
			}
		}

		if (groups != null && !groups.isEmpty()) {
			for (int j = 0; j < table.getRows().size(); j++) {
				String val = "";
				for (Integer i : is)
					val += "@" + table.getRows().get(j).getValue(i);
				if (!groupAgg.containsKey(val))
					groupAgg.put(val, new Tuple(tuple.getValues()));
				Tuple t = groupAgg.get(val);
				Table t2 = new Table();
				t2.addRow(table.getRows().get(j));
				t2.setSchema(table.getSchema());
				for (int i = 0; i < aggregates.size(); i++) {
					Table t1 = aggregation(t2, aggregates.get(i), t.getValue(i));
					t.setValue(i, t1.getValue(0, 0));
				}
				groupAgg.put(val, t);
			}
		} else {
			if(groupAgg.isEmpty())
				groupAgg.put("", new Tuple(tuple.getValues()));
			for (int j = 0; j < table.getRows().size(); j++) {
				Tuple t = groupAgg.get("");
				Table t2 = new Table();
				t2.addRow(table.getRows().get(j));
				t2.setSchema(table.getSchema());
				for (int i = 0; i < aggregates.size(); i++) {
					Table t1 = aggregation(t2, aggregates.get(i), t.getValue(i));
					t.setValue(i, t1.getValue(0, 0));
				}
				groupAgg.put("", t);
			}
		}
		return res;
	}

	private Table aggregation(Table t, AggColumn agg, LeafValue initialVal) {
		if (agg.aggType == AType.AVG)
			return average(t, agg, (StringValue) initialVal);
		if (agg.aggType == AType.COUNT)
			return count(t, agg, (LongValue) initialVal);
		if (agg.aggType == AType.COUNT_DISTINCT)
			return countDistinct(t, agg, (LongValue) initialVal);
		if (agg.aggType == AType.MAX)
			return max(t, agg, (DoubleValue) initialVal);
		if (agg.aggType == AType.MIN)
			return min(t, agg, (DoubleValue) initialVal);
		if (agg.aggType == AType.SUM)
			return sum(t, agg, (DoubleValue) initialVal);
		return null;
	}

	private Table min(Table t, AggColumn agg, DoubleValue initialVal) {
		double min = initialVal.getValue();
		Expression ex = agg.expr[0];
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			try {
				eval.next();
				LeafValue val = eval.eval(ex);
				if (val instanceof DoubleValue) {
					double v = ((DoubleValue) val).getValue();
					if (v < min)
						min = v;
				}
				if (val instanceof LongValue) {
					long v = ((LongValue) val).getValue();
					if (v < min)
						min = v;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new Table(new DoubleValue(min), ColumnType.DOUBLE, agg.name);
	}

	private Table max(Table t, AggColumn agg, DoubleValue initialVal) {
		double max = initialVal.getValue();
		Expression ex = agg.expr[0];
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			try {
				eval.next();
				LeafValue val = eval.eval(ex);
				if (val instanceof DoubleValue) {
					double v = ((DoubleValue) val).getValue();
					if (v > max)
						max = v;
				}
				if (val instanceof LongValue) {
					long v = ((LongValue) val).getValue();
					if (v > max)
						max = v;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new Table(new DoubleValue(max), ColumnType.DOUBLE, agg.name);
	}

	private Table countDistinct(Table t, AggColumn agg, LongValue initialVal) {
		String name = ((Column) agg.expr[0]).getWholeColumnName();
		int col = t.getSchema().getColIndex(name);
		Set<LeafValue> s = new HashSet<LeafValue>();
		for (int i = 0; i < t.getRows().size(); i++)
			s.add(t.getRows().get(i).getValue(col));
		return new Table(new LongValue(s.size()), ColumnType.INT, agg.name);
	}

	private Table count(Table t, AggColumn agg, LongValue initialVal) {
		long count = t.getRows().size() + initialVal.getValue();
		return new Table(new LongValue(count), ColumnType.INT, agg.name);
	}

	private Table sum(Table t, AggColumn agg, DoubleValue initialVal) {
		double sum = initialVal.getValue();
		Expression ex = agg.expr[0];
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			try {
				eval.next();
				LeafValue val = eval.eval(ex);
				if (val instanceof DoubleValue) {
					sum += ((DoubleValue) val).getValue();
				}
				if (val instanceof LongValue) {
					sum += ((LongValue) val).getValue();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new Table(new DoubleValue(sum), ColumnType.DOUBLE, agg.name);
	}

	private Table average(Table t, AggColumn agg, StringValue ival) {
		String initialVal = ival.getValue();
		int count = Integer.parseInt(initialVal.substring(0,
				initialVal.indexOf(':')));
		double sum = Double.parseDouble(initialVal.substring(initialVal
				.indexOf(':') + 1));
		Expression ex = agg.expr[0];
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			count++;
			try {
				eval.next();
				LeafValue val = eval.eval(ex);
				if (val instanceof DoubleValue) {
					sum += ((DoubleValue) val).getValue();
				}
				if (val instanceof LongValue) {
					sum += ((LongValue) val).getValue();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new Table(new StringValue("'" + count + ":" + sum + "'"),
				ColumnType.STRING, agg.name);
	}

}
