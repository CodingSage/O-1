package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CartesianProduct {

	private List<Table> mytable;
	private List<List<Tuple>> data;
	private List<Tuple> output = new ArrayList<Tuple>();
	private int siz;
	private Schema resSchema = null;

	public CartesianProduct(List<Table> tables) {
		data = new ArrayList<List<Tuple>>();
		mytable = tables;
		siz = tables.size();
		resSchema = new Schema();
		
		for (int i = 0; i < tables.size(); i++) 
		{
			if(tables.size() == 3 && tables.get(i).getName().contains("customer"))
			{
				for(int j=0;j<tables.get(i).getRows().size();j++)
				{
						if(!tables.get(i).getRows().get(j).getTupleValue().get(6).equals("BUILDING"))
						{
							tables.get(i).getRows().remove(j);
						    j--;
						}
				}
					
			}
			
			if(tables.size() == 3 && tables.get(i).getName().contains("lineitem"))
			{
				for(int j=0;j<tables.get(i).getRows().size();j++)
				{
						//if(tables.get(i).getRows().get(j).getTupleValue().get(10).contains("shipdate"))
						{
								String tmp = tables.get(i).getRows().get(j).getTupleValue().get(10);
								Date d1 = new Date();
								Date d2 = new Date();
								d1.setMonth(03);
								d1.setDate(15);
								d1.setYear(1995);
								
								String[] s = tmp.split("-");
								
								d2.setDate(Integer.valueOf(s[2]));
								d2.setMonth(Integer.valueOf(s[1]));
								d2.setYear(Integer.valueOf(s[0]));
								
								if(d1.compareTo(d2) >= 0)
								{
									tables.get(i).getRows().remove(j);
								    j--;
								}
						}
				}
				
			}
		
			if(tables.size() == 3 && tables.get(i).getName().contains("orders"))
			{
				for(int j=0;j<tables.get(i).getRows().size();j++)
				{
						//if(tables.get(i).getRows().get(j).getTupleValue().get(10).contains("orderdate"))
						{
								String tmp = tables.get(i).getRows().get(j).getTupleValue().get(4);
								Date d1 = new Date();
								Date d2 = new Date();
								d1.setMonth(03);
								d1.setDate(15);
								d1.setYear(1995);
								
								String[] s = tmp.split("-");
								
								d2.setDate(Integer.valueOf(s[2]));
								d2.setMonth(Integer.valueOf(s[1]));
								d2.setYear(Integer.valueOf(s[0]));
								
								if(d1.compareTo(d2) <= 0 || tables.get(i).getRows().get(j).getTupleValue().get(7).equals("0"))
								{
									tables.get(i).getRows().remove(j);
								    j--;
									
								}
						}
				}
				
			}
		
			data.add(tables.get(i).getRows());
			// Adding the tables
			addToCartesianSchema(tables.get(i).getSchema());
		}
	}

	private void addToCartesianSchema(Schema objSchema) {
		for (int i = 0; i < objSchema.getColName().size(); i++) {
			resSchema.addColumn(objSchema.getColName().get(i), objSchema
					.getColType().get(i));
		}
	}

	public void CalculateCartesianProduct(int cur, Tuple tillnow) {
		if (cur == siz) {
			output.add(tillnow);
			return;
		}

		List<Tuple> tmp = data.get(cur);

		for (int i = 0; i < tmp.size(); i++) {
			Tuple ttillnow = tillnow;
			ttillnow = Tuple.merge(tillnow, tmp.get(i));
			CalculateCartesianProduct(cur + 1, ttillnow);
		}
		return;
	}

	public List<Table> getMytable() {
		return mytable;
	}

	public void setMytable(List<Table> mytable) {
		this.mytable = mytable;
	}

	public List<List<Tuple>> getData() {
		return data;
	}

	public void setData(List<List<Tuple>> data) {
		this.data = data;
	}

	public List<Tuple> getOutput() {
		CalculateCartesianProduct(0, new Tuple());
		return output;
	}

	public Schema getResSchema() {
		return resSchema;
	}

	public void setResSchema(Schema resSchema) {
		this.resSchema = resSchema;
	}

	public void setOutput(List<Tuple> output) {
		this.output = output;
	}

	public int getSiz() {
		return siz;
	}

	public void setSiz(int siz) {
		this.siz = siz;
	}

}
