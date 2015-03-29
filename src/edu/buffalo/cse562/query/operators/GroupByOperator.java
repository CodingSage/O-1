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

import edu.buffalo.cse562.checkpoint1.AggregateNode.AggColumn;
import edu.buffalo.cse562.checkpoint1.ProjectionNode.Target;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.FileFunction;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;

public class GroupByOperator extends Operator {

	private List<Target> GroupByParameters;
	private Table ResultTableName;
	public Table GroupedByTable;
	String line = null;
	public Map<String, Integer> FilesList = new HashMap<String, Integer>();
	public Map<String, List<Tuple>> hmGroupedTuple = new HashMap<String, List<Tuple>>();

	public GroupByOperator(Table t, List<Target> _target,
			List<AggColumn> _AggCol) {
		ResultTableName = t;
		GroupByParameters = _target;
	}

	protected Table evaluate() {

		if (DataManager.getInstance().getStoragePath() != null
				&& !(DataManager.getInstance().getStoragePath().equals(""))) {

			return extEvaluate();

		} else {
			//extEvaluate();
			return inMemoryEvaluate();
            
		}
		// return GroupedByTable;
	}

	protected Table extEvaluate() {

		FileReader filereader;
		try {
			StringBuilder fileName = new StringBuilder();
			fileName.append(DataManager.getInstance().getDataPath())
					.append(ResultTableName.getName().toUpperCase())
					.append(".dat");
			filereader = new FileReader(fileName.toString());

			GroupedByTable = new Table();
			GroupedByTable.setName(ResultTableName.getName() + "groupedby");
			GroupedByTable.setSchema(ResultTableName.getSchema());

			int x = GroupByParameters.size();

			BufferedReader bufferedReader = new BufferedReader(filereader);
			FileFunction fHandle = new FileFunction();
			while ((line = bufferedReader.readLine()) != null) {
				String[] values = line.split("\\|");
				StringBuilder newfile = new StringBuilder();

				for (int i = 0; i < x; i++) {
					int ind = ResultTableName.getSchema().getColIndex(
							GroupByParameters.get(i).expr.toString()
									.toLowerCase());
					newfile.append(DataManager.getInstance().getDataPath());
					newfile.append(values[ind]);
				}

				if (FilesList.get(newfile.toString()) == null)
					FilesList.put(newfile.toString(), 1);

				StringBuilder lin = new StringBuilder();

				lin.append(line).append("\n");
				// given filename should be able to write to it
				fHandle.getWriter(newfile.toString()).write(lin.toString());
			}
			filereader.close();
			fHandle.closeBuffers();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		Iterator<Entry<String, Integer>> it = FilesList.entrySet().iterator();
		FileFunction res = new FileFunction();
		BufferedWriter bwlocal = null;
		bwlocal = res.getWriter(DataManager.getInstance().getDataPath()
				+ GroupedByTable.getName());
		while (it.hasNext()) {
			Map.Entry<String, Integer> Pair = (Entry<String, Integer>) it
					.next();
			try {
				filereader = new FileReader(Pair.getKey());
				BufferedReader bufferedreader = new BufferedReader(filereader);

				try {
					while ((line = bufferedreader.readLine()) != null) {
						StringBuilder lin = new StringBuilder();
						lin.append(line).append("\n");
						bwlocal.write(lin.toString());

					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				filereader.close();

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				// res.closeBuffers();
			}
			// res.closeBuffers();
		}
		try {
			bwlocal.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Loop through all the files in the swap folder and delete the files
//		Iterator<Entry<String, Integer>> iter = FilesList.entrySet().iterator();
//		while (iter.hasNext()) {
//			Map.Entry<String, Integer> Pair = iter.next();
//			File file = new File(Pair.getKey());
//			file.delete();
//		}
		GroupedByTable.FilesList = FilesList ;
		return GroupedByTable;
	}

	protected Table inMemoryEvaluate() {

		GroupedByTable = new Table();
		GroupedByTable.setName(ResultTableName.getName() + "groupedby");
		GroupedByTable.setSchema(ResultTableName.getSchema());

		int x = GroupByParameters.size();

		for (Tuple row : ResultTableName.getRows()) {

			String[] values = row.toString().split("\\|");
			StringBuilder newhashKey = new StringBuilder();

			for (int i = 0; i < x; i++) {

				int ind = ResultTableName.getSchema().getColIndex(
						GroupByParameters.get(i).expr.toString().toLowerCase());
				newhashKey.append("Grouped").append(values[ind]);

			}

			if (hmGroupedTuple.get(newhashKey.toString()) == null) {
				ArrayList<Tuple> lsTuple = new ArrayList<Tuple>();
				lsTuple.add(row);
				hmGroupedTuple.put(newhashKey.toString(), lsTuple);
			} else {
				hmGroupedTuple.get(newhashKey.toString()).add(row);
			}
		}

		Iterator<Entry<String, List<Tuple>>> iterator = hmGroupedTuple
				.entrySet().iterator();

		while (iterator.hasNext()) {

			Map.Entry<String, List<Tuple>> mpEntry = iterator.next();
			for(Tuple row : mpEntry.getValue()) {
				GroupedByTable.addRow(row);
			}
		}
//		BufferedWriter bwlocal = null;
//		FileFunction re = new FileFunction();
//		bwlocal = re.getWriter(DataManager.getInstance().getDataPath()
//				+ "TestFiledata");
//		try {
//			bwlocal.write(GroupedByTable.toString());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println(GroupedByTable.toString());
//        System.out.println(GroupedByTable.getRows().size());
		
		return GroupedByTable;
	}
}
