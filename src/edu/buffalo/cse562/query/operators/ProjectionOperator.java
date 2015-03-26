package edu.buffalo.cse562.query.operators;

import java.sql.SQLException;
import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;
import edu.buffalo.cse562.query.Evaluator;

public class ProjectionOperator extends Operator {

	private List<Target> targets;
	private Table table;
	
	public ProjectionOperator(Table t, List<Target> cols) {
		table = t;
		targets = cols;
	}
	
	@Override
	protected Table evaluate() {
		// TODO projection implementations
		
		Table res = new Table();
		
		int siz = targets.size();
		
		for(int i=0; i<siz; i++)
		{
				Target cur = targets.get(i);
				Expression e = cur.expr;
				//if(e != null)
				{
							Evaluator eval = new Evaluator(table);
							Table tmp = new Table();
							while(eval.hasNext())
							{
									try
									{
											eval.next();
											LeafValue val = eval.eval(cur.expr);
											Tuple add = new Tuple();
											LeafValue value ;
											if(val instanceof LongValue){
												value = (LongValue)val ;
												add.insertColumn(String.valueOf(((LongValue) value).getValue()));
											}
											else if(val instanceof DoubleValue){
												value = (DoubleValue)val;
												add.insertColumn(String.valueOf(((DoubleValue)value).getValue()));
											}
											else add.insertColumn(val.toString());
											
											tmp.addRow(add);
									}
									catch(SQLException e1)
									{
											e1.printStackTrace();
									}
							}
							res.addTableColumn((tmp));
				}
				
//				else
//				{
//						res.addTableColumn(table.getColumn(cur.name));	 
//				}
		}
		res.FilesList = table.FilesList;
		return res;
	}

}
