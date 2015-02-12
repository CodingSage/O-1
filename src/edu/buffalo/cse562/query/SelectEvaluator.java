package edu.buffalo.cse562.query;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.CartesianProduct;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;

public class SelectEvaluator implements SelectVisitor, FromItemVisitor {

	Table result;
	List<Table> tables;
	
	public Table getResult() {
		return result;
	}

	@Override
	public void visit(PlainSelect select) {
		FromItem item = select.getFromItem();
		item.accept(this);
		//TODO joins
		CartesianProduct prod = new CartesianProduct(tables);
		List<ArrayList<Tuple>> result = prod.getOutput();
	}

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(net.sf.jsqlparser.schema.Table table) {
		tables.add(DataManager.getInstance().getTable(table.getName()));
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub
		
	}

}
