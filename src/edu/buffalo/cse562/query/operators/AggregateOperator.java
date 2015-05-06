package edu.buffalo.cse562.query.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.DateValue;
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
	private static Map<String, List<Object>> groupAgg = new HashMap<String, List<Object>>();
	private static Schema schema = null;
	private static List<Integer> is = null;
	private static List<Object> tuple = null;

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
				List<Object> tupleValues = groupAgg.get(key);
				Tuple tup1 = listToTuple(tupleValues);
				Tuple a = Tuple.merge(tup, tup1);
				for(Integer i : avgIs){
					
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
			tuple = new ArrayList<Object>();
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
					tuple.add(Arrays.asList(0.0, 0.0));
				else if (col.aggType == AType.MIN)
					tuple.add(Double.MAX_VALUE);
				else if (col.aggType == AType.MAX || col.aggType == AType.SUM)
					tuple.add(0.0);
				else
					tuple.add(0);
				schema.addColumn(col.name, type);
			}
		}

		if (groups != null && !groups.isEmpty()) {
			for (int j = 0; j < table.getRows().size(); j++) {
				String val = "";
				for (Integer i : is)
					val += "@" + table.getRows().get(j).getValue(i);
				if (!groupAgg.containsKey(val))
					groupAgg.put(val, new ArrayList<Object>(tuple));
				List<Object> t = groupAgg.get(val);
				Table t2 = new Table();
				t2.addRow(table.getRows().get(j));
				t2.setSchema(table.getSchema());
				for (int i = 0; i < aggregates.size(); i++) {
					Object o = aggregation(t2, aggregates.get(i), t.get(i));
					t.set(i, o);
				}
				groupAgg.put(val, t);
			}
		} else {
			if (groupAgg.isEmpty())
				groupAgg.put("", new ArrayList<Object>(tuple));
			for (int j = 0; j < table.getRows().size(); j++) {
				List<Object> t = groupAgg.get("");
				Table t2 = new Table();
				t2.addRow(table.getRows().get(j));
				t2.setSchema(table.getSchema());
				for (int i = 0; i < aggregates.size(); i++) {
					Object o = aggregation(t2, aggregates.get(i), t.get(i));
					t.set(i, o);
				}
				groupAgg.put("", t);
			}
		}
		return res;
	}

	private Tuple listToTuple(List<Object> tupleValues) {
		List<LeafValue> leaves = new ArrayList<LeafValue>();
		for (Object obj : tupleValues) {
			if (obj instanceof String)
				leaves.add(new StringValue("'" + obj.toString() + "'"));
			else if (obj instanceof Double)
				leaves.add(new DoubleValue((double) obj));
			else if (obj instanceof Integer)
				leaves.add(new LongValue((Integer) obj));
			else if (obj instanceof Date)
				leaves.add(new DateValue("'" + obj.toString() + "'"));
			else if(obj instanceof List){
				List<Object> list = (List<Object>) obj;
				Double avg = ((Double)list.get(1))/((Double)list.get(0));
				leaves.add(new DoubleValue(avg));
			}
		}
		return new Tuple(leaves);
	}

	private Object aggregation(Table t, AggColumn agg, Object initialVal) {
		if (agg.aggType == AType.AVG)
			return average(t, agg, (List<Double>) initialVal);
		if (agg.aggType == AType.COUNT)
			return count(t, agg, (Integer) initialVal);
		if (agg.aggType == AType.COUNT_DISTINCT)
			return countDistinct(t, agg, (Integer) initialVal);
		if (agg.aggType == AType.MAX)
			return max(t, agg, (Double) initialVal);
		if (agg.aggType == AType.MIN)
			return min(t, agg, (Double) initialVal);
		if (agg.aggType == AType.SUM)
			return sum(t, agg, (Double) initialVal);
		return null;
	}

	private Object min(Table t, AggColumn agg, Double initialVal) {
		Double min = initialVal;
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
						min = (double) v;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return min;
	}

	private Object max(Table t, AggColumn agg, Double initialVal) {
		double max = initialVal;
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
		return max;
	}

	private Object countDistinct(Table t, AggColumn agg, Integer initialVal) {
		String name = ((Column) agg.expr[0]).getWholeColumnName();
		int col = t.getSchema().getColIndex(name);
		Set<LeafValue> s = new HashSet<LeafValue>();
		for (int i = 0; i < t.getRows().size(); i++)
			s.add(t.getRows().get(i).getValue(col));
		return s.size();
	}

	private Object count(Table t, AggColumn agg, Integer initialVal) {
		int count = t.getRows().size() + initialVal;
		return count;
	}

	private Object sum(Table t, AggColumn agg, Double initialVal) {
		double sum = initialVal;
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
		return sum;
	}

	private Object average(Table t, AggColumn agg, List<Double> ival) {
		Double count = ival.get(0);
		Double sum = ival.get(1);
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
		return Arrays.asList(count, sum);
	}

}
