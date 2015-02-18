package edu.buffalo.cse562.query;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.statement.Statement;

public class Query {

	private Statement query; 
	
	public Query(Statement query){
		this.query = query;
	}
	
	public void evaluate(){
		StatementEvaluator evaluator = new StatementEvaluator();
		query.accept(evaluator);
		Table result = evaluator.getResult();
		if(result != null)
			System.out.print(result.toString());
	}
	
}
