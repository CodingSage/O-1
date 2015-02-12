
package org.sample;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Table;

public class CartesianProduct {
	
			List<Table> mytable;
			List<ArrayList<Tuple>> data;
			List<Tuple> output;
			int siz;
			
			public CartesianProduct(List<Table> tables)
			{
					mytable = tables;
					siz = tables.size();
					for(int i=0;i<tables.size();i++)
							data.add(tables[i].rows);
			}
			
			public void CalculateCartesianProduct(int cur, Tuple tillnow)
			{	
					if(cur == siz)
					{
							output.add(tillnow);
							return ;
					}
					
					ArrayList<Tuple> tmp = data[cur];
					
					for(int i=0;i<tmp.size();i++)
							doit(cur + 1, tillnow.append(tmp[i]));
						
					return ;
			}
}

