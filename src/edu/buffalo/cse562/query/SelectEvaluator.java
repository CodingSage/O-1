package edu.buffalo.cse562.query;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.model.Tuple;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

public class SelectEvaluator implements SelectVisitor {

	public List<Tuple> getResult() {
		// TODO return results
		return new ArrayList<Tuple>();
	}

	@Override
	public void visit(PlainSelect select) {
		FromItem item = select.getFromItem();
	}

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub

	}

}
