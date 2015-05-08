package edu.buffalo.cse562.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.core.DataManager;

public class FileFunction {

	
	private static FileWriter fw = null;
	private static BufferedWriter bfw = null;
	private static String fname = null;
	private static HashMap<String, BufferedWriter> hfileWriters = new HashMap<String,BufferedWriter>();
	private static Map<String, Cursor> readers = new HashMap<String, Cursor>();
	private static Map<String, Database> dbs = new HashMap<String, Database>();
	private static EnvironmentConfig envConfig = null;
	private static Environment environment = null;
	
	public FileFunction() {
	}

	FileFunction(String fileName){
	}
	
	public static void addReadTable(String tableName){
		try{
			if(envConfig == null)
				envConfig = new EnvironmentConfig();
			if(environment == null)
				environment = new Environment(new File(DataManager.getInstance().getStoragePath()), envConfig);
			DatabaseConfig dbConfig = new DatabaseConfig();
			Database db = environment.openDatabase(null, tableName, dbConfig);
			dbs.put(tableName, db);
			Cursor cursor = db.openCursor(null, null);
			readers.put(tableName, cursor);
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static Tuple readTable(String tableName){
		if(!readers.containsKey(tableName))
			addReadTable(tableName);
		Cursor cursor = readers.get(tableName);
		if(cursor == null)
			return null;
		Schema schema = DataManager.getInstance().getSchema(tableName);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		Tuple tuple = null;
		try {
			if(cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				byte[] tupData = value.getData();
				ByteArrayInputStream in = new ByteArrayInputStream(tupData);
				DataInputStream data = new DataInputStream(in);
				tuple = new Tuple();
				for (int i = 0; i < schema.getColName().size(); i++) {
					ColumnType type = schema.getColType().get(i);
					if (type == ColumnType.CHAR || type == ColumnType.STRING
							|| type == ColumnType.VARCHAR)
						tuple.insertColumn(new StringValue("'" + data.readUTF() + "'"));
					else if (type == ColumnType.DATE)
						tuple.insertColumn(new DateValue("'" + data.readUTF() + "'"));
					else if (type == ColumnType.DECIMAL || type == ColumnType.DOUBLE)
						tuple.insertColumn(new DoubleValue(data.readDouble()));
					else if (type == ColumnType.INT)
						tuple.insertColumn(new LongValue(data.readInt()));
				}
			} else {
				readers.get(tableName).close();
				readers.put(tableName, null);
				dbs.get(tableName).close();
				dbs.put(tableName, null);
				if(dbs.size() == DataManager.getInstance().getTableCount()){
					for(String a : dbs.keySet()){
						if(dbs.get(a) != null){
							return tuple;
						}
					}
					environment.close();
				}
			}
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tuple;
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
