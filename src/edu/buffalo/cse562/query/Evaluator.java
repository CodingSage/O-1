package edu.buffalo.cse562.query;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;

public class Evaluator extends Eval implements Iterator<Tuple> {

	private List<String> tableNames;
	private Table operand;
	private int curIndex;

	public Evaluator(Table table, List<String> tableNames) {
		operand = table;
		this.tableNames = tableNames;
		curIndex = -1;
	}

	@Override
	public LeafValue eval(Column arg) throws SQLException {
		String col = arg.getColumnName();
		String tableName = arg.getTable().getName();
		if (tableName != null)
			col =  tableName + "$" + col;
		int i = -1;
		String type = "";
		// TODO check multiple same tables
		int offset = 0;
		for (String table : tableNames) {
			Schema schema = DataManager.getInstance().getSchema(table);
			i = schema.getColIndex(col);
			if (i != -1) {
				type = schema.getType(col);
				i += offset;
				break;
			}
			offset += schema.getNumberColumns();
		}
		Tuple current = operand.getRows().get(curIndex);
		String s = current.getValue(i);
		// TODO check other types
		if (type.toLowerCase().equals("int"))
			return new LongValue(s);
		if (type.toLowerCase().equals("double"))
			return new DoubleValue(s);
		if (type.toLowerCase().equals("date"))
			return new DateValue(s);
		if (type.equals("string"))
			return new StringValue(s);
		return new NullValue();
	}

	public Table getOperand() {
		return operand;
	}

	public void setOperand(Table operand) {
		this.operand = operand;
	}

	@Override
	public boolean hasNext() {
		return curIndex + 1 < operand.getRows().size();
	}

	@Override
	public Tuple next() {
		return operand.getRows().get(++curIndex);
	}

	public void reset() {
		curIndex = -1;
	}

	@Override
	public void remove() {
		operand.getRows().remove(curIndex);
		curIndex--;
	}

}
