package edu.buffalo.cse562.query;

import edu.buffalo.cse562.checkpoint1.PlanNode;
import edu.buffalo.cse562.model.Table;

public class Query {

	private PlanNode raTree;

	public Query(PlanNode query) {
		this.raTree = query;
	}

	public void evaluate() {
		optimizeTree(raTree);
		Table result = null;
		if (result != null) {
			System.out.print(result.toString());
			/*
			 * try { PrintWriter writer = new
			 * PrintWriter("/home/vinayak/ans.txt", "UTF-8");
			 * writer.print(result.toString()); writer.close(); } catch
			 * (FileNotFoundException | UnsupportedEncodingException e) {
			 * e.printStackTrace(); }
			 */
		}
	}

	private void optimizeTree(PlanNode raTree2) {
		// TODO Auto-generated method stub

	}

}
