package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.checkpoint1.PlanNode;
import edu.buffalo.cse562.checkpoint1.SqlToRA;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.query.Query;
import edu.buffalo.cse562.query.StatementEvaluator;

public class Main {

	public static void main(String[] args) {
		/*
		 * System.out.print(
		 * "We, the members of our team, agree that we will not submit any code "
		 * +
		 * "that we have not written ourselves, share our code with anyone outside of "
		 * +
		 * "our group, or use code that we have not written ourselves as a reference."
		 * );
		 */
		List<File> sqlFiles = new ArrayList<File>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].contains("--data")) {
				i++;
				DataManager.getInstance().setDataPath(args[i]);
			} else if (args[i].contains("--swap")) {
				i++;
				DataManager.getInstance().setStoragePath(args[i]);
			} else
				sqlFiles.add(new File(args[i]));
		}
		evaluate(sqlFiles);
	}

	//REATE TABLE LINEITEM (orderkey INT , partkey INT , suppkey
	//>>>  INT , linenumber INT , quantity DECIMAL , extendedprice DECIMAL , discount DECI
	//>>> MAL , tax DECIMAL , returnflag CHAR (1) , linestatus CHAR (1) , shipdate DATE ,
	//>>> commitdate DATE , receiptdate DATE , shipinstruct CHAR (25) , shipmode CHAR (10)
	//>>>  , comment VARCHAR (44) )
	public static Table pre()
	{
		Table lineitem = new Table("LINEITEM");
		Schema s = new Schema();
		s.addColumn("orderkey", "int");
		s.addColumn("partkey", "int");
		s.addColumn("suppkey", "int");
		s.addColumn("linenumber", "int");
		s.addColumn("quantity", "decimal");
		s.addColumn("extendedprice", "decimal");
		s.addColumn("discount", "decimal");
		s.addColumn("tax", "decimal");
		s.addColumn("returnflag", "char");
		s.addColumn("linestatus", "char");
		s.addColumn("shipdate", "date");
		s.addColumn("commitdate", "date");
		s.addColumn("receiptdate", "date");
		s.addColumn("shipinstruct", "varchar");
		s.addColumn("shipmode", "varchar");
		s.addColumn("comment", "varchar");
		return lineitem;
	}
	

	private static void evaluate(List<File> sqlFiles) {
	
		Table lineitem = pre();
		
		for (File file : sqlFiles) {
			try {
				FileReader reader = new FileReader(file);
				Statement statement = null;
				SqlToRA translator = new SqlToRA();
				CCJSqlParser parser = new CCJSqlParser(reader);																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																
				PlanNode plan = null;
				// TODO union implementation	
		
				while ((statement = parser.Statement()) != null) 
				{    
					System.out.println("My known tables - " + translator.getKnownTables());
					if (statement instanceof CreateTable) 
					{  
						String newName = ((CreateTable) statement).getTable().getName().toUpperCase();
						((CreateTable) statement).getTable().setName(newName);
						translator.loadTableSchema((CreateTable) statement);
						statement.accept(new StatementEvaluator());
					} 
					else 	
					{
						//plan = translator.selectToPlan(((Select) statement).getSelectBody());
						//System.out.println(plan);
						//System.out.println("------------------------------");
						Select selectStatement = (Select)statement;
						SelectBody s = selectStatement.getSelectBody();
						System.out.println(translator.selectToPlan(s));
						Query query = new Query(translator.selectToPlan(s));
						query.evaluate();
					}
				}
				
				//Query query = new Query(plan);
				//query.evaluate();
				reader.close();
			} 
			catch (Exception e) 
			{
				System.out.println(file.toString());
				e.printStackTrace();
			}
		}
	}
}
