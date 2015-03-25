package edu.buffalo.cse562.query.operators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.FileHandler;
import java.sql.SQLException;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.model.FileFunction;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.Evaluator;
public class GroupByOperator extends Operator {

	private List<Target> GroupByParameters;
	private Table ResultTableName;
	String line = null;
	public Map<String, Integer> FilesList = new HashMap<String, Integer>();

	
	public GroupByOperator(Table t, List<Target> _target)
	{
		ResultTableName = t;
		GroupByParameters = _target;
	}	

	protected Table evaluate() 
	{
		
		FileReader filereader;
		try 
		{
			filereader = new FileReader(ResultTableName.toString());
	
			BufferedReader bufferedReader = new BufferedReader(filereader);
			FileFunction fHandle = new FileFunction();
			try {
			
				while((line = bufferedReader.readLine()) != null)
				{
								String[] values = line.split("|");
								String newfilename = null; 
								
								int siz = GroupByParameters.size();
								
								for(int i=0;i<siz;i++)
								{
										int ind = ResultTableName.getSchema().getColIndex(GroupByParameters.get(i).toString()); 
										newfilename += values[ind];
								}
								
								if(FilesList.get(newfilename) == null)
										FilesList.put(newfilename, 1);
								
								StringBuilder lin = new StringBuilder();
								lin.append(line).append("\n");
								fHandle.getWriter(newfilename).write(lin.toString());  //given filename should be able to write to it
								
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			filereader.close();
		} 
		catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Iterator it = FilesList.entrySet().iterator();
		
		FileFunction res = new FileFunction();
		
		while(it.hasNext())
		{
					Map.Entry<String, Integer> Pair = (Entry<String, Integer>) it.next();
					
					try 
					{
						filereader = new FileReader(Pair.getKey());
						BufferedReader bufferedreader = new BufferedReader(filereader);
						
						try 
						{
							while((line = bufferedreader.readLine()) != null)
							{
								StringBuilder lin = new StringBuilder();
								lin.append(line).append("\n");
								res.getWriter(ResultTableName.getName()).write(line);
								
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					} 
					catch (FileNotFoundException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
		
			
		}
		
		return ResultTableName;
	}

}

