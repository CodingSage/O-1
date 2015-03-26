package edu.buffalo.cse562.query.operators;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
import edu.buffalo.cse562.checkpoint1.AggregateNode.AggColumn;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.FileFunction;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.Evaluator;
public class GroupByOperator extends Operator {

	private List<Target> GroupByParameters;
	private List<AggColumn> AggColumn ;
	private Table ResultTableName;
	public Table GroupedByTable;
	String line = null;
	public Map<String, Integer> FilesList = new HashMap<String, Integer>();

	
	public GroupByOperator(Table t, List<Target> _target,List<AggColumn> _AggCol)
	{
		ResultTableName = t;
		GroupByParameters = _target;
		AggColumn = _AggCol;
	}	

	protected Table evaluate() 
	{
		
		FileReader filereader;
		try 
		{
			
			StringBuilder fileName = new StringBuilder();
			fileName.append(DataManager.getInstance().getDataPath()).append(ResultTableName.getName().toUpperCase()).append(".dat");
			//filereader = new FileReader(ResultTableName.toString());
			filereader = new FileReader(fileName.toString());
			GroupedByTable = new Table();
			GroupedByTable.setName(ResultTableName.getName() + "groupedby");
			//GroupedByTable.setName(ResultTableName.getName() + "groupedby");

			int x = GroupByParameters.size();
			
			List<String> colnames = ResultTableName.getSchema().getColName();
			List<String> coltypes = ResultTableName.getSchema().getColType();
			
			int[] chosen = new int[colnames.size()];
			
			for(int i=0;i<colnames.size();i++)
					chosen[i] = 0;
			String s = GroupByParameters.get(0).expr.toString();
			String g = GroupByParameters.get(0).name;
			for(int i=0;i<x;i++)
			{
					for(int j=0;j<colnames.size();j++)
					{
						if(GroupByParameters.get(i).expr.toString().toLowerCase().compareTo(colnames.get(j)) == 0)
						{
								GroupedByTable.getSchema().addColumn(colnames.get(j), coltypes.get(j));
								chosen[j] = 1;
								break;
						}
					}
			}
			
			
			for(int j=0;j<colnames.size();j++)
					if(chosen[j] == 0)
					{	
								GroupedByTable.getSchema().addColumn(colnames.get(j), coltypes.get(j));
					}
			
			
			BufferedReader bufferedReader = new BufferedReader(filereader);
			FileFunction fHandle = new FileFunction();
			
			try {

				while((line = bufferedReader.readLine()) != null)
				{
								String[] values = line.split("\\|");
								String newfilename = null; 
								StringBuilder newfile = new StringBuilder();
								String groupedby = null, nongroupedby = null;
								StringBuilder groupedby1 = new StringBuilder();
								StringBuilder nongroupedby1 = new StringBuilder();
								
								int siz = GroupByParameters.size();
								
								for(int i=0;i<colnames.size();i++)
										if(chosen[i] == 0){
											nongroupedby += values[i];
											if(i!= colnames.size()-1)
											nongroupedby1.append(values[i]).append("|");
											else
											nongroupedby1.append(values[i])	;
										}
												
								
								
								for(int i=0;i<x;i++)
								{
										int ind = ResultTableName.getSchema().getColIndex(GroupByParameters.get(i).expr.toString().toLowerCase()); 
										newfilename += values[ind];
										newfile.append(DataManager.getInstance().getDataPath());
										newfile.append(values[ind]);
										groupedby1.append(values[ind]).append("|");
										groupedby += values[ind];
								}
								
			
								if(FilesList.get(newfile.toString()) == null)
										FilesList.put(newfile.toString(), 1);
								
								StringBuilder lin = new StringBuilder();
								lin.append(groupedby1.toString()).append(nongroupedby1.toString()).append("\n");
								fHandle.getWriter(newfile.toString()).write(lin.toString());  //given filename should be able to write to it
								
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			filereader.close();
			fHandle.closeBuffers();
		} 
		catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Iterator it = FilesList.entrySet().iterator();
		
		FileFunction res = new FileFunction();
		BufferedWriter bwlocal = null;
		bwlocal =res.getWriter(DataManager.getInstance().getDataPath()+ GroupedByTable.getName());
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
								bwlocal.write(lin.toString());
								
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						filereader.close();
						
					} 
					catch (FileNotFoundException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}finally{
						
						//res.closeBuffers();
					}
					
		
			//res.closeBuffers();
		}
		try {
			bwlocal.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Loop through all the files in the swap folder and delete the files
		
		Iterator iter = FilesList.entrySet().iterator();
		
		while(iter.hasNext()){
			
			Map.Entry<String, Integer> Pair = (Entry<String, Integer>) iter.next();
			File file = new File(Pair.getKey());
			file.delete();
		}
		return GroupedByTable;
	}

}

