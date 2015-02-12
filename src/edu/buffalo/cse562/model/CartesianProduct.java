
package edu.buffalo.cse562.model;

import java.util.ArrayList;
import java.util.List;

public class CartesianProduct {

                        List<Table> mytable;
                        List<ArrayList<Tuple>> data;
                        List<ArrayList<Tuple>> output;
                        int siz;

                        public CartesianProduct(List<Table> tables)
                        {
                                        mytable = tables;
                                        siz = tables.size();
                                        for(int i=0;i<tables.size();i++)
                                                        data.add((ArrayList<Tuple>) tables.get(i).rows);
                        }
                        
                        public void CalculateCartesianProduct(int cur, ArrayList<Tuple> tillnow)
                        {
                                        if(cur == siz)
                                        {
                                                        output.add(tillnow);
                                                        return ;
                                        }

                                        ArrayList<Tuple> tmp = data.get(cur);

                                        for(int i=0;i<tmp.size();i++)
                                        {
                                        	ArrayList<Tuple> ttillnow = tillnow;
                                        	ttillnow.add(tmp.get(i));
                                        	CalculateCartesianProduct(cur + 1, ttillnow);
                                        }
                                        return ;
                        }
}

