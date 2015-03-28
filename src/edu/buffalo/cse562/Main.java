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

	private static void evaluate(List<File> sqlFiles) {
		for (File file : sqlFiles) {
			try {
				FileReader reader = new FileReader(file);
				Statement statement = null;
				SqlToRA translator = new SqlToRA();
				CCJSqlParser parser = new CCJSqlParser(reader);
				PlanNode plan = null;
				// TODO union implementation
				while ((statement = parser.Statement()) != null) {    
					if (statement instanceof CreateTable) {  
						String newName = ((CreateTable) statement).getTable().getName().toUpperCase();
						((CreateTable) statement).getTable().setName(newName);
						translator.loadTableSchema((CreateTable) statement);
						System.out.println(translator.getKnownTables() + " bug");
						statement.accept(new StatementEvaluator());
					} else 
					{
						//String newP = ((Select)statement).getSelectBody().toString().toUpperCase();
						
						plan = translator.selectToPlan(((Select) statement).getSelectBody());
						//System.out.println(plan);
						//System.out.println("------------------------------");
					}
				}
				Query query = new Query(plan);
				query.evaluate();
				reader.close();
			} catch (Exception e) {
				System.out.println(file.toString());
				e.printStackTrace();
			}
		}
	}
}
