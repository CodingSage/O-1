package edu.buffalo.cse562.model;


public abstract class Operator {

	protected Table table;

	protected abstract Table evaluate();

	public Table execute() {
		/*Table res = new Table();
		String name = DataManager.getInstance().assignFileName();
		res.setName(name);
		res.append(evaluate());
		return res;*/
		return evaluate();
	}
}
