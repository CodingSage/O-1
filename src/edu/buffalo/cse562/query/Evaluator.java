package edu.buffalo.cse562.query;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Constants;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;

public class Evaluator extends Eval implements Iterator<Tuple> {

	private List<String> tableNames;
	private Table operand;
	private int curIndex;

	public Evaluator(Table table) {
		operand = table;
		curIndex = -1;
	}

	@Override
	public LeafValue eval(Column arg) throws SQLException {
		String col = arg.getColumnName();
		String tableName = arg.getTable().getName();
		if (tableName != null)
			col = tableName + Constants.COLNAME_DELIMITER + col;
		int i = -1;
		Schema schema = operand.getSchema();
		i = schema.getColIndex(col);
		Tuple current = operand.getRows().get(curIndex);
		LeafValue s = current.getValue(i);
		return s;
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

	public List<String> getTableNames() {
		return tableNames;
	}

}
