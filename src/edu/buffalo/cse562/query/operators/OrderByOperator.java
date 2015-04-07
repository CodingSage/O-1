package edu.buffalo.cse562.query.operators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import edu.buffalo.cse562.checkpoint1.SortNode.Ordering;
import edu.buffalo.cse562.core.DataManager;
import edu.buffalo.cse562.model.FileFunction;
import edu.buffalo.cse562.model.Operator;
import edu.buffalo.cse562.model.Schema;
import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.model.Tuple;
import edu.buffalo.cse562.model.Utilities;

public class OrderByOperator extends Operator {

	private List<Ordering> OrderbyParameters;
	private Table ResultTableName, OrderedTableName;
	String line = null;
	public Map<String, Integer> OrderedFilesList = new HashMap<String, Integer>();
	public Map<String, Integer> FilesList = new HashMap<String, Integer>();

	public OrderByOperator(Table t, List<Ordering> _OrderByParameters) {

		ResultTableName = t;
		OrderbyParameters = _OrderByParameters;
		FilesList = t.FilesList;
	}

	public OrderByOperator(Table t, List<Ordering> _OrderByParameters,
			Map<String, Integer> _FilesList) {
		ResultTableName = t;
		OrderbyParameters = _OrderByParameters;
		FilesList = t.FilesList;
	}

	@Override
	protected Table evaluate() {
		// TODO projection implementations
		if (DataManager.getInstance().getStoragePath() != null
				&& !(DataManager.getInstance().getStoragePath().equals(""))) {

			return extEvaluate();

		} else {
			// extEvaluate();
			return inMemoryEvaluate();

		}
	}

	protected Table inMemoryEvaluate() {
		Map<Integer, Integer> isdesc = new HashMap<Integer, Integer>();	
		
		int siz = OrderbyParameters.size();
		
		/*for (int i = 0; i < siz; i++) 
		{
			String colName = OrderbyParameters.get(i).expr.toString();
			int ind = ResultTableName.getSchema().getColIndex(colName);
			if (OrderbyParameters.get(i).ascending == false)
				isdesc.put(ind, 1);
		}*/
		
		TreeMap<List<String>, Tuple> sortedlist = new TreeMap<List<String>, Tuple>(
				new ValueComparator(isdesc));
		
		Schema schema = ResultTableName.getSchema();
		for (int j = 0; j < ResultTableName.getRows().size(); j++) {
			Tuple cur = ResultTableName.getRows().get(j);
			List<LeafValue> values = cur.getValues();
			List<String> keyadd = new ArrayList<String>();
			for (int i = 0; i < siz; i++) {
				String colName = OrderbyParameters.get(i).expr.toString();
				int ind = ResultTableName.getSchema().getColIndex(colName);
				if (OrderbyParameters.get(i).ascending == false)
					isdesc.put(keyadd.size(), 1);
				LeafValue v = cur.getValue(ind);
				String val = "";
				if(v instanceof DateValue)
					val = ((DateValue)v).getValue().toString();
				if(v instanceof LongValue)
					val = ((LongValue)v).getValue() + "";
				if(v instanceof DoubleValue)
					val = ((DoubleValue)v).getValue() + "";
				else
					val = v.toString();
				keyadd.add(val);
			}
			sortedlist.put(keyadd, cur);
		}

		OrderedTableName = new Table();
		OrderedTableName.setName(ResultTableName.getName() + "orderedBy");
		OrderedTableName.setSchema(ResultTableName.getSchema());
		OrderedTableName.setSchema(ResultTableName.getSchema());
		Iterator it = sortedlist.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<List<String>, Tuple> Pair = (Entry<List<String>, Tuple>) it.next();
			OrderedTableName.addRow(Pair.getValue());
		}

		return OrderedTableName;

	}

	protected Table extEvaluate() {

		FileReader filereader;
		FileFunction f1 = new FileFunction();

		Iterator it = FilesList.entrySet().iterator(); // get the files list
														// from Group By

		while (it.hasNext()) {
			Map.Entry<String, Integer> Pair = (Entry<String, Integer>) it
					.next();
			try {
				filereader = new FileReader(Pair.getKey());
				BufferedReader bufferedreader = new BufferedReader(filereader);
				Map<Integer, Integer> isdesc = new HashMap<Integer, Integer>();

				TreeMap<List<String>, String> sortedlist = new TreeMap<List<String>, String>(
						new ValueComparator(isdesc));
				try {
					while ((line = bufferedreader.readLine()) != null) {
						int siz = OrderbyParameters.size();
						List<String> values = Utilities.splitStrings('|', line);// line.split("\\|");
						List<String> keyadd = new ArrayList<String>();
						for (int i = 0; i < siz; i++) {

							int ind = ResultTableName.getSchema().getColIndex(
									OrderbyParameters.get(i).expr.toString());
							keyadd.add(values.get(ind));
						}

						sortedlist.put(keyadd, line);
					}

					filereader.close();

					String Filename = Pair.getKey() + "Ordered";

					if (OrderedFilesList.get(Filename) == null)
						OrderedFilesList.put(Filename, 1);

					Iterator it2 = sortedlist.entrySet().iterator();

					while (it2.hasNext()) {
						Map.Entry<List<String>, String> Pair2 = (Entry<List<String>, String>) it2
								.next();

						f1.getWriter(Filename).write(Pair2.getValue());
						it2.remove();
					}

					// f1.close();
				}

				catch (IOException e) {
					e.printStackTrace();
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			it.remove();
		}

		try {
			mergefiles();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return OrderedTableName;

	}

	class ValueComparator implements Comparator<List<String>> {

		Map<Integer, Integer> base;

		public ValueComparator(Map<Integer, Integer> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with
		// equals.
		public int compare(List<String> a, List<String> b) {
			int x = a.size(), y = b.size();

			for (int i = 0; i < x; i++) {
				double v1, v2;
				Date d1 = new Date();
				Date d2 = new Date();

				String[] aa = a.get(i).split("-");
				String[] bb = b.get(i).split("-");

				if (aa.length == 3 && bb.length == 3) {
					d1.setYear(Integer.valueOf(aa[0]));
					d1.setMonth(Integer.valueOf(aa[1]));
					d1.setDate(Integer.valueOf(aa[2]));

					d2.setYear(Integer.valueOf(bb[0]));
					d2.setMonth(Integer.valueOf(bb[1]));
					d2.setDate(Integer.valueOf(bb[2]));

					if (base.containsKey(i)) {
						if (d1.compareTo(d2) < 0)
							return 1;
						else if (d1.compareTo(d2) > 0)
							return -1;
					}

					if (d1.compareTo(d2) > 0)
						return 1;
					if (d1.compareTo(d2) < 0)
						return -1;
				}
				if (!a.get(i).contains("-") && a.get(i).charAt(0) >= '0'
						&& a.get(i).charAt(0) <= '9') {
					v1 = Double.parseDouble(a.get(i));
					v2 = Double.parseDouble(b.get(i));

					if (base.containsKey(i)) {
						if (v1 < v2)
							return 1;
						else if (v1 > v2)
							return -1;
					}

					if (v1 > v2)
						return 1;
					else if (v1 < v2)
						return -1;
				} else {

					if (base.containsKey(i)) {
						if (a.get(i).compareTo(b.get(i)) < 0)
							return 1;
						else if (a.get(i).compareTo(b.get(i)) > 0)
							return -1;

					}
					if (a.get(i).compareTo(b.get(i)) > 0)
						return 1;
					else if (a.get(i).compareTo(b.get(i)) < 0)
						return -1;
				}
			}
			return -1;
			// returning 0 would merge keys
		}
	}

	class ValueComparator2 implements Comparator<List<String>> {

		Map<Integer, Integer> base;

		public ValueComparator2(Map<Integer, Integer> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with
		// equals.
		public int compare(List<String> a, List<String> b) {
			int x = a.size() - 2, y = b.size() - 2;

			for (int i = 0; i < x; i++) {
				double v1, v2;
				Date d1 = new Date();
				Date d2 = new Date();

				String[] aa = a.get(i).split("-");
				String[] bb = b.get(i).split("-");

				if (aa.length == 3 && bb.length == 3) {
					d1.setYear(Integer.valueOf(aa[0]));
					d1.setMonth(Integer.valueOf(aa[1]));
					d1.setDate(Integer.valueOf(aa[2]));

					d2.setYear(Integer.valueOf(bb[0]));
					d2.setMonth(Integer.valueOf(bb[1]));
					d2.setDate(Integer.valueOf(bb[2]));

					if (base.containsKey(i)) {
						if (d1.compareTo(d2) < 0)
							return 1;
						else if (d1.compareTo(d2) > 0)
							return -1;
					}

					if (d1.compareTo(d2) > 0)
						return 1;
					if (d1.compareTo(d2) < 0)
						return -1;
				}
				if (!a.get(i).contains("-") && a.get(i).charAt(0) >= '0'
						&& a.get(i).charAt(0) <= '9') {
					v1 = Double.parseDouble(a.get(i));
					v2 = Double.parseDouble(b.get(i));

					if (base.containsKey(i)) {
						if (v1 < v2)
							return 1;
						else if (v1 > v2)
							return -1;
					}

					if (v1 > v2)
						return 1;
					else if (v1 < v2)
						return -1;
				} else {

					if (base.containsKey(i)) {
						if (a.get(i).compareTo(b.get(i)) < 0)
							return 1;
						else if (a.get(i).compareTo(b.get(i)) > 0)
							return -1;

					}
					if (a.get(i).compareTo(b.get(i)) > 0)
						return 1;
					else if (a.get(i).compareTo(b.get(i)) < 0)
						return -1;
				}
			}
			return -1;
			// returning 0 would merge keys
		}
	}

	protected void mergefiles() throws IOException {
		Iterator it = OrderedFilesList.entrySet().iterator();

		OrderedTableName.setName(ResultTableName.getName() + "ordered");
		OrderedTableName.setSchema(ResultTableName.getSchema());

		int siz = OrderedFilesList.size(), siz2 = OrderbyParameters.size();

		int[] indexes = new int[siz];

		for (int i = 0; i < siz; i++)
			indexes[i] = 0;

		PriorityQueue<List<String>> pq = new PriorityQueue<List<String>>(
				new ValueComparator2(new HashMap<Integer, Integer>()));

		int cur = 0;

		FileFunction res = new FileFunction();

		while (it.hasNext()) {
			Map.Entry<String, Integer> Pair = (Entry<String, Integer>) it
					.next();
			FileReader fr = new FileReader(Pair.getKey());
			BufferedReader br = new BufferedReader(fr);
			List<String> params = new ArrayList<String>();
			line = br.readLine();
			if (line != null) {
				List<String> values = Utilities.splitStrings('|', line);// line.split("|");
				for (int i = 0; i < siz2; i++) {
					int ind = ResultTableName.getSchema().getColIndex(
							OrderbyParameters.get(i).toString());
					params.add(values.get(ind));
				}
				params.add(line);
				params.add(cur + "");
				pq.add(params);
				indexes[cur] += 1;
			}
			fr.close();
			cur++;
			it.remove();
		}

		while (!pq.isEmpty()) {

			List<String> top = pq.poll();

			pq.remove();

			int tot = top.size();
			int ind = Integer.parseInt(top.get(tot - 1));

			res.getWriter(OrderedTableName.getName()).write(top.get(tot - 2));

			cur = 0;

			it = OrderedFilesList.entrySet().iterator();

			while (it.hasNext()) {
				if (cur == ind) {
					Map.Entry<String, Integer> Pair = (Entry<String, Integer>) it
							.next();

					FileReader fr = new FileReader(Pair.getKey());
					BufferedReader br = new BufferedReader(fr);

					int st = 0;

					List<String> params = new ArrayList<String>();

					while (st < indexes[cur] && (line = br.readLine()) != null)
						st++;

					line = br.readLine();

					if (line != null) {
						String[] values = line.split("|");

						for (int i = 0; i < siz2; i++) {
							int ind1 = ResultTableName.getSchema().getColIndex(
									OrderbyParameters.get(i).toString());
							params.add(values[ind1]);
						}

						params.add(line);
						params.add(cur + "");
						pq.add(params);
						indexes[cur] += 1;
					}

					fr.close();
					break;
				}
				cur++;
				it.remove();
			}

		}

	}

}
