package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.query.Query;

public class Main {

	public static void main(String[] args) {
		System.out
				.print("We, the members of our team, agree that we will not submit any code "
						+ "that we have not written ourselves, share our code with anyone outside of "
						+ "our group, or use code that we have not written ourselves as a reference.");
		List<File> sqlFiles = new ArrayList<File>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].contains("--data")) {
				i++;
				DataManager.getInstance().setDataPath(args[i]);
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
				CCJSqlParser parser = new CCJSqlParser(reader);
				while ((statement = parser.Statement()) != null) {
					new Query(statement).evaluate();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
