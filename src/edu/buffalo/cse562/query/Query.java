package edu.buffalo.cse562.query;

import net.sf.jsqlparser.statement.Statement;
import edu.buffalo.cse562.model.Table;

public class Query {

	private Statement query;

	public Query(Statement query) {
		this.query = query;
	}

	public void evaluate() {
		StatementEvaluator evaluator = new StatementEvaluator();
		query.accept(evaluator);
		Table result = evaluator.getResult();
		if (result != null) {
			System.out.print(result.toString());
			/*try {
				PrintWriter writer = new PrintWriter("/home/vinayak/ans.txt", "UTF-8");
				writer.print(result.toString());
				writer.close();
			} catch (FileNotFoundException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}*/
		}
	}

}
