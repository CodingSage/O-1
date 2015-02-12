package edu.buffalo.cse562.query;

import net.sf.jsqlparser.statement.Statement;

public class Query {

	private Statement query; 
	
	public Query(Statement query){
		this.query = query;
	}
	
	public void evaluate(){
		StatementEvaluator evaluator = new StatementEvaluator();
		query.accept(evaluator);
	}
	
}
