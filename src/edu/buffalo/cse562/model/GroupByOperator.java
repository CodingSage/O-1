package edu.buffalo.cse562.model;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupByOperator {

		List<String> GroupByParameters = new ArrayList<String>();
		Map<String, Integer> FilesList = new HashMap<String, Integer>();
		Table ResultTableName;
		String line = null;
		
		void CalculateGroupBy()
		{			
				FileReader filereader;
				try {
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
												int ind = ResultTableName.getSchema().getColIndex(GroupByParameters.get(i)); 
												newfilename += values[ind];
										}
										
										if(FilesList.get(newfilename) == null)
												FilesList.put(newfilename, 1);
										
										StringBuilder lin = new StringBuilder();
										lin.append(line).append("\n");;
										fHandle.getWriter(newfilename).write(lin.toString());  //given filename should be able to write to it
										
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			
		}
		
}