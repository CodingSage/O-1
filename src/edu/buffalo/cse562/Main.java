package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
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
			} else if (args[i].contains("--db")) {
				i++;
				DataManager.getInstance().setStoragePath(args[i]);
			} else if (args[i].contains("--load")) {
				//load phase
			} else
				sqlFiles.add(new File(args[i]));
		}
		evaluate(sqlFiles);
	}

	private static void evaluate(List<File> sqlFiles) {
		SqlToRA translator = new SqlToRA();
		//System.out.println(new Date(System.currentTimeMillis()));
		for (File file : sqlFiles) {
			try {
				FileReader reader = new FileReader(file);
				Statement statement = null;
				CCJSqlParser parser = new CCJSqlParser(reader);
				// TODO union implementation
				while ((statement = parser.Statement()) != null) {
					try {
						throw new Exception(statement.toString());
					} catch (Exception e) {
						e.printStackTrace();
					}
					/*
					if (statement instanceof CreateTable) {
						String newName = ((CreateTable) statement).getTable()
								.getName().toUpperCase();
						((CreateTable) statement).getTable().setName(newName);
						translator.loadTableSchema((CreateTable) statement);
						statement.accept(new StatementEvaluator());
					} else if (statement instanceof Select) {
						Select selectStatement = (Select) statement;
						SelectBody s = selectStatement.getSelectBody();
						Query query = new Query(translator.selectToPlan(s));
						query.evaluate();
					}
					*/
				}
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		//System.out.println(new Date(System.currentTimeMillis()));
	}
}
