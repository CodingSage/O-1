package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;

public class Utilities {

	public static LeafValue toLeafValue(String value, ColumnType type) {
		if (type == ColumnType.CHAR || type == ColumnType.STRING || type == ColumnType.VARCHAR)
			return new StringValue("'" + value + "'");
		else if (type == ColumnType.DATE)
			return new DateValue("'" + value + "'");
		else if (type == ColumnType.DECIMAL || type == ColumnType.DOUBLE)
			return new DoubleValue(value);
		else if (type == ColumnType.INT)
			return new LongValue(value);
		else
			return new NullValue();
	}

	public static List<String> splitStrings(char split, String str){
		List<String> datas = new ArrayList<String>();
		int pos = 0, end;
		while((end = str.indexOf(split, pos)) >= 0){
			datas.add(str.substring(pos, end));
			pos = end+1;
		}
		datas.add(str.substring(pos));
		return datas;
	}

}
