package edu.buffalo.cse562.model;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;
import java.util.HashMap;

public class FileFunction {

	
	public static FileWriter fw = null;
	public static BufferedWriter bfw = null;
	public static String fname = null;
	public static HashMap<String,BufferedWriter> hfileWriters = new HashMap<String,BufferedWriter>();
	
	public FileFunction() {
		
	}

	FileFunction(String fileName){

		
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
					// TODO Auto-generated catch block
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
