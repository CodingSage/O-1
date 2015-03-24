package edu.buffalo.cse562.model;

import edu.buffalo.cse562.core.DataManager;

public abstract class Operator {

	protected Table table;

	protected abstract Table evaluate();

	public Table execute() {
		Table res = new Table();
		String name = DataManager.getInstance().assignFileName();
		if (name.isEmpty())
			res = evaluate();
		else {
			table.loadData();
			res.setName(name);
			while (table.isEmpty()) {
				res.append(evaluate());
				table.loadData();
			}
		}
		return res;
	}
}
