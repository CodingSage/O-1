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
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.checkpoint1.AggregateNode.AType;
import edu.buffalo.cse562.checkpoint1.AggregateNode.AggColumn;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;
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
		if(table == null){
			Table res = new Table();
			List<Integer> avgIs = new ArrayList<Integer>();
			
			for(int i = 0; i < aggregates.size(); i++){
				if(aggregates.get(i).aggType == AType.AVG)
					avgIs.add(i);
			}
			
			for(String key : groupAgg.keySet())
			{
				String[] cols = key.split("@");
				Tuple tup = new Tuple();
				for(int i = 1; i < cols.length; i++)
					tup.insertColumn(cols[i]);
				Tuple a = Tuple.merge(tup, groupAgg.get(key));
				for(Integer i : avgIs)
				{
					String[] v = a.getValue(groups.size()+i).split(":");
					double avg = Double.parseDouble(v[1])/Integer.parseInt(v[0]);
					a.setValue(groups.size()+i, avg + "");
				}
				res.addRow(a);
			}
			groupAgg.clear();
			res.setSchema(schema);
			schema = null;
			return res;
		}
		
		Table res = new Table();
		if(table.isEmpty())
			return res;
		if(schema == null){
			Schema s = table.getSchema();
			schema = new Schema();
			is = new ArrayList<Integer>();
			tuple = new Tuple();
			if (groups != null) {
				for (Target g : groups) {
					String colName = ((Column) g.expr).getWholeColumnName();
					String colType = s.getType(colName);
					schema.addColumn(colName, colType);
					int i = s.getColIndex(colName);
					is.add(i);
				}
			}
			for(AggColumn col : aggregates){
				String type = "";
				if(col.aggType == AType.AVG || col.aggType == AType.SUM || col.aggType == AType.MAX || col.aggType == AType.MIN) {
					type = "double";
				} else {
					type = "int";
				}
				if(col.aggType == AType.AVG)
					tuple.insertColumn("0:0");
				else if(col.aggType == AType.MIN)
					tuple.insertColumn("" + Double.MAX_VALUE);
				else
					tuple.insertColumn("0");
				schema.addColumn(col.name, type);
			}
		}
		
		if (groups != null && !groups.isEmpty()) {
			//only one tuple being sent
			String val = "";
			for (Integer i : is)
				val += "@" + table.getRows().get(0).getValue(i);
			
			if(!groupAgg.containsKey(val))
				groupAgg.put(val, new Tuple(tuple.getValues()));
			
			Tuple t = groupAgg.get(val);
			
			for(int i = 0; i < aggregates.size(); i++)
			{
				Table t1 = aggregation(table, aggregates.get(i), t.getValue(i));
				t.setValue(i, t1.getValue(0, 0));
			}
			
			groupAgg.put(val, t);
		}
		return res;
	}

	private Table aggregation(Table t, AggColumn agg, String initialVal) {
		if (agg.aggType == AType.AVG)
			return average(t, agg, initialVal);
		if (agg.aggType == AType.COUNT)
			return count(t, agg, initialVal);
		if (agg.aggType == AType.COUNT_DISTINCT)
			return countDistinct(t, agg, initialVal);
		if (agg.aggType == AType.MAX)
			return max(t, agg, initialVal);
		if (agg.aggType == AType.MIN)
			return min(t, agg, initialVal);
		if (agg.aggType == AType.SUM)
			return sum(t, agg, initialVal);
		return null;
	}

	private Table min(Table t, AggColumn agg, String initialVal) {
		double min = Double.parseDouble(initialVal);
		Expression ex = agg.expr[0];
		String type = "";
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			try {
				eval.next();
				LeafValue val = eval.eval(ex);
				if (val instanceof DoubleValue) {
					double v = ((DoubleValue) val).getValue();
					if(v < min)
						min = v;
					type = "double";
				}
				if (val instanceof LongValue) {
					long v = ((LongValue) val).getValue();
					if(v < min)
						min = v;
					type = "int";
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new Table("" + min, type, agg.name);
	}

	private Table max(Table t, AggColumn agg, String initialVal) {
		double max = Double.parseDouble(initialVal);
		Expression ex = agg.expr[0];
		String type = "";
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			try {
				eval.next();
				LeafValue val = eval.eval(ex);
				if (val instanceof DoubleValue) {
					double v = ((DoubleValue) val).getValue();
					if(v > max)
						max = v;
					type = "double";
				}
				if (val instanceof LongValue) {
					long v = ((LongValue) val).getValue();
					if(v > max)
						max = v;
					type = "int";
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new Table("" + max, type, agg.name);
	}

	private Table countDistinct(Table t, AggColumn agg, String initialVal) {
		String name = ((Column) agg.expr[0]).getWholeColumnName();
		int col = t.getSchema().getColIndex(name);
		Set<String> s = new HashSet<String>();
		for (int i = 0; i < t.getRows().size(); i++)
			s.add(t.getRows().get(i).getValue(col));
		return new Table("" + s.size(), "int", agg.name);
	}

	private Table count(Table t, AggColumn agg, String initialVal) {
		int count = t.getRows().size() + Integer.parseInt(initialVal);
		return new Table("" + count, "int", agg.name);
	}

	private Table sum(Table t, AggColumn agg, String initialVal) {
		double sum = Double.parseDouble(initialVal);
		Expression ex = agg.expr[0];
		String type = "";
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			try {
				eval.next();
				LeafValue val = eval.eval(ex);
				if (val instanceof DoubleValue) {
					sum += ((DoubleValue) val).getValue();
					type = "double";
				}
				if (val instanceof LongValue) {
					sum += ((LongValue) val).getValue();
					type = "int";
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new Table("" + sum, type, agg.name);
	}

	private Table average(Table t, AggColumn agg, String initialVal) {
		String[] vals = initialVal.split(":");
		int count = Integer.parseInt(vals[0]);
		double sum = Double.parseDouble(vals[1]);
		Expression ex = agg.expr[0];
		String type = "";
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			count++;
			try {
				eval.next();
				LeafValue val = eval.eval(ex);
				if (val instanceof DoubleValue) {
					sum += ((DoubleValue) val).getValue();
					type = "double";
				}
				if (val instanceof LongValue) {
					sum += ((LongValue) val).getValue();
					type = "double";
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new Table(count+ ":" + sum, type, agg.name);
	}

}
