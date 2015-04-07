package edu.buffalo.cse562.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.core.Optimizer;

public class FileFunction {

	
	private static FileWriter fw = null;
	private static BufferedWriter bfw = null;
	private static String fname = null;
	private static HashMap<String, BufferedWriter> hfileWriters = new HashMap<String,BufferedWriter>();
	private static Map<String, BufferedReader> readers = new HashMap<String, BufferedReader>();
	
	public FileFunction() {
	}

	FileFunction(String fileName){
	}
	
	public static void addReadTable(String tableName){
		BufferedReader reader = null;
		try {
			//TODO check path for swap folder
			File file = new File(DataManager.getInstance().getDataPath()
					+ File.separator + tableName + ".dat");
			FileReader fileread = new FileReader(file);
			reader = new BufferedReader(fileread);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		readers.put(tableName, reader);
	}
	
	public static Tuple readTable(String tableName){
		if(!readers.containsKey(tableName))
			addReadTable(tableName);
		BufferedReader reader = readers.get(tableName);
		Tuple row = new Tuple();
		try {
			Schema schema = DataManager.getInstance().getSchema(tableName);
			if(!reader.ready())
				return null;
			String line = reader.readLine();
			if(line == null){
				reader.close();
				return null;
			}
			List<String> s = Optimizer.splitStrings('|', line);
			for(int i = 0; i < schema.getColName().size(); i++) {
				ColumnType type = schema.getColType().get(i);
				LeafValue val = Utilities.toLeafValue(s.get(i), type);
				row.insertColumn(val);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return row;
	}
	
	public  BufferedWriter getWriter(String fileName){
	     FileWriter fwlocal;
	     BufferedWriter bwlocal = null;
	     
			if(!hfileWriters.isEmpty() && hfileWriters.get(fileName)!=null )return hfileWriters.get(fileName);
			else{
			    try {
					fwlocal = new FileWriter(fileName);
					bwlocal = new BufferedWriter(fwlocal);
					hfileWriters.put(fileName, bwlocal);
					return bwlocal ;
				} catch (IOException e) {
					e.printStackTrace();
			  }
		}
	    return bwlocal;		
	  }

		public  void closeBuffers(){
			//iterating over values only
			for (BufferedWriter value : hfileWriters.values()) {
			    try {
					value.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

//	public static boolean FileWriter(String fileName,String row){
//		
//		File file = new File(fileName);
//		try{
//			  BufferedWriter bfw = new BufferedWriter(fw); 
//			  bfw.write(row);
//			
//		}catch(IOException ioException){
//			ioException.printStackTrace();
//		}
//		return true;
//	}
}
