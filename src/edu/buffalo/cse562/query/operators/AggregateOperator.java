package edu.buffalo.cse562.query.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

	public AggregateOperator(List<Target> groupCols,
			List<AggColumn> aggColumns, Table a) {
		groups = groupCols;
		table = a;
		aggregates = aggColumns;
	}

	@Override
	protected Table evaluate() {
		Table res = new Table();
		Schema schema = new Schema();
		Schema s = table.getSchema();
		List<Integer> is = new ArrayList<Integer>();
		if (groups != null) {
			for (Target g : groups) {
				String colName = ((Column) g.expr).getWholeColumnName();
				int i = s.getColIndex(colName);
				is.add(i);
			}
		}
		if (groups != null && !groups.isEmpty()) {
			String val = "";
			for (Integer i : is)
				val += table.getRows().get(0).getValue(i);
			Table t = new Table();
			t.setSchema(s);
			t.addRow(table.getRows().get(0));
			for (int j = 1; j < table.getRows().size(); j++) {
				String rowVal = "";
				for (Integer i : is){
					rowVal += table.getRows().get(j).getValue(i);
				}
				while (val.equals(rowVal))
					t.addRow(table.getRows().get(j));
				t.setSchema(s);
				val = rowVal;
				Tuple tuple = new Tuple();
				Tuple tupl = t.getRows().get(0); 
				for(Integer i  : is)
					tuple.insertColumn(tupl.getValue(i));
				for (AggColumn agg : aggregates) {
					Table tres = aggregation(t, agg);
					schema.addSchema(tres.getSchema());
					tuple.insertColumn(tres.getRows().get(0).getValue(0));
				}
				res.addRow(tuple);
				t = new Table();
				t.addRow(table.getRows().get(j));
			}
		} else {
			for (AggColumn agg : aggregates) {
				Table tres = aggregation(table, agg);
				schema.addSchema(tres.getSchema());
				res.addTableColumn(tres);
			}
		}
		res.setSchema(schema);
		return res;
	}

	private Table aggregation(Table t, AggColumn agg) {
		if (agg.aggType == AType.AVG)
			return average(t, agg);
		if (agg.aggType == AType.COUNT)
			return count(t, agg);
		if (agg.aggType == AType.COUNT_DISTINCT)
			return countDistinct(t, agg);
		if (agg.aggType == AType.MAX)
			;
		if (agg.aggType == AType.MIN)
			;
		if (agg.aggType == AType.SUM)
			return sum(t, agg);
		return null;
	}
	
	private Table max(Table t, AggColumn agg){
		return null;
	}
	
	private Table countDistinct(Table t, AggColumn agg){
		String name = ((Column)agg.expr[0]).getWholeColumnName();
		int col = t.getSchema().getColIndex(name);
		Set<String> s = new HashSet<String>();
		for(int i = 0; i < t.getRows().size(); i++)
			s.add(t.getRows().get(i).getValue(col));
		return new Table("" + s.size(), "int", agg.name);
	}

	private Table count(Table t, AggColumn agg) {
		int count = t.getRows().size();
		return new Table("" + count, "int", agg.name);
	}

	private Table sum(Table t, AggColumn agg) {
		double sum = 0;
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

	private Table average(Table t, AggColumn agg) {
		int count = 0;
		double sum = 0;
		Expression ex = agg.expr[0];
		String type = "";
		Evaluator eval = new Evaluator(t);
		while (eval.hasNext()) {
			count++;
			try {
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
		return new Table("" + sum / count, type, agg.name);
	}

}
