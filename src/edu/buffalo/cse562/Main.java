package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.crypto.Data;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.buffalo.cse562.checkpoint1.SqlToRA;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.ColumnType;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Tuple;
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
		
		boolean loadPhase = false;
		List<File> sqlFiles = new ArrayList<File>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].contains("--data")) {
				i++;
				DataManager.getInstance().setDataPath(args[i]);
			} else if (args[i].contains("--db")) {
				i++;
				DataManager.getInstance().setStoragePath(args[i]);
			} else if (args[i].contains("--load")) {
				loadPhase = true;
			} else
				sqlFiles.add(new File(args[i]));
		}
		evaluate(sqlFiles, loadPhase);
	}

	private static void evaluate(List<File> sqlFiles, boolean loadPhase) {
		SqlToRA translator = new SqlToRA();
		//System.out.println(new Date(System.currentTimeMillis()));
		for (File file : sqlFiles) {
			try {
				FileReader reader = new FileReader(file);
				Statement statement = null;
				CCJSqlParser parser = new CCJSqlParser(reader);
				// TODO union implementation
				while ((statement = parser.Statement()) != null) {
					if (statement instanceof CreateTable) {
						String newName = ((CreateTable) statement).getTable()
								.getName().toUpperCase();
						((CreateTable) statement).getTable().setName(newName);
						translator.loadTableSchema((CreateTable) statement);
						statement.accept(new StatementEvaluator());
						if(loadPhase)
							createDB(newName);
					} else if (statement instanceof Select) {
						Select selectStatement = (Select) statement;
						SelectBody s = selectStatement.getSelectBody();
						Query query = new Query(translator.selectToPlan(s));
						query.evaluate();
					}
				}
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		//System.out.println(new Date(System.currentTimeMillis()));
	}

	private static void createDB(String tableName) {
		Environment environment = null;
		Database db = null;
		Cursor cursor = null;
		DataManager instance = DataManager.getInstance();
		try {
			Schema schema = instance.getSchema(tableName);
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			environment = new Environment(new File(instance.getStoragePath()), envConfig);
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			db = environment.openDatabase(null, tableName, dbConfig);

			FileReader fileread = new FileReader(instance.getDataPath() + File.separator + tableName + ".dat");
			BufferedReader buffReader = new BufferedReader(fileread);
			String line;
			Tuple row = new Tuple();
			int rowNum = 0;
			while ((line = buffReader.readLine()) != null) {
				rowNum++;
				String[] s = line.split("\\|");

				ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dataOut = new DataOutputStream(out);

				for (int i = 0; i < schema.getColName().size(); i++) {
					ColumnType type = schema.getColType().get(i);
					if (type == ColumnType.CHAR || type == ColumnType.STRING
							|| type == ColumnType.VARCHAR || type == ColumnType.DATE)
						dataOut.writeUTF(s[i]);
					else if (type == ColumnType.DECIMAL || type == ColumnType.DOUBLE)
						dataOut.writeDouble(Double.parseDouble(s[i]));
					else if (type == ColumnType.INT)
						dataOut.writeInt(Integer.parseInt(s[i]));
				}
				byte[] tupleData = out.toByteArray();
				String skey = s[schema.getPrimaryKeyIndex()];
				DatabaseEntry key = new DatabaseEntry(skey.getBytes("UTF-8"));
				DatabaseEntry value = new DatabaseEntry(tupleData);
				db.put(null, key, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (cursor != null)
				cursor.close();
			if (db != null)
				db.close();
			if (environment != null)
				environment.close();
		}
	}
}
