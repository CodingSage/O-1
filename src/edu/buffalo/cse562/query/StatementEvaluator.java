package edu.buffalo.cse562.query;

import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;

public class StatementEvaluator implements StatementVisitor {

	public Table result;

	public Table getResult() {
		return result;
	}

	@Override
	public void visit(Select select) {
		SelectEvaluator eval = new SelectEvaluator();
		select.getSelectBody().accept(eval);
		result = eval.getResult();
	}

	@Override
	public void visit(Delete arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Update arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Insert arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Replace arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Drop arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Truncate arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CreateTable table) {
		String name = table.getTable().getWholeTableName();
		Table t = new Table(name);
		Schema schema = new Schema();
		for (Object def : table.getColumnDefinitions()) {
			ColumnDefinition defn = (ColumnDefinition) def;
			String type = defn.getColDataType().getDataType();
			schema.addColumn(defn.getColumnName(), type);
		}
		DataManager instance = DataManager.getInstance();
		instance.addNewTable(schema, t);
	}

}
