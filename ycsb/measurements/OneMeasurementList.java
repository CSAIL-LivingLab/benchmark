/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package simpledb.versioned.benchmark.ycsb.measurements;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import simpledb.versioned.benchmark.ycsb.exporter.MeasurementsExporter;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 * 
 * @author cooperb
 *
 */
public class OneMeasurementList extends OneMeasurement
{
	int operations;
	long totallatency;
	
	int min;
	int max;
	HashMap<Integer,int[]> returncodes;

	List<Integer> _values;

	public OneMeasurementList(String name, Properties props)
	{
		super(name);
		_values = new ArrayList<Integer>();
		operations=0;
		totallatency=0;
		
		min=-1;
		max=-1;
		returncodes=new HashMap<Integer,int[]>();
	}

	/* (non-Javadoc)
	 * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
	 */
	public synchronized void reportReturnCode(int code)
	{
		Integer Icode=code;
		if (!returncodes.containsKey(Icode))
		{
			int[] val=new int[1];
			val[0]=0;
			returncodes.put(Icode,val);
		}
		returncodes.get(Icode)[0]++;
	}


	/* (non-Javadoc)
	 * @see com.yahoo.ycsb.OneMeasurement#measure(int)
	 */
	public synchronized void measure(int latency)
	{
		_values.add(latency);

		operations++;
		totallatency+=latency;

		if ( (min<0) || (latency<min) )
		{
			min=latency;
		}

		if ( (max<0) || (latency>max) )
		{
			max=latency;
		}
	}


  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
    exporter.write(getName(), "Operations", operations);
    exporter.write(getName(), "AverageLatency(ms)", (((double)totallatency)/((double)operations)));
    exporter.write(getName(), "MinLatency(ms)", min);
    exporter.write(getName(), "MaxLatency(ms)", max);
    
    int opcounter=0;
    boolean done95th=false;
    List<Integer> _valuesSorted = new ArrayList<Integer>();
    Collections.sort(_valuesSorted);
    for (int val : _valuesSorted)
    {
      opcounter+=1;
      if ( (!done95th) && (((double)opcounter)/((double)operations)>=0.95) )
      {
        exporter.write(getName(), "95thPercentileLatency(ms)", val);
        done95th=true;
      }
      if (((double)opcounter)/((double)operations)>=0.99)
      {
        exporter.write(getName(), "99thPercentileLatency(ms)", val);
        break;
      }
    }

    for (Integer I : returncodes.keySet())
    {
      int[] val=returncodes.get(I);
      exporter.write(getName(), "Return="+I, val[0]);
    }     

    for (int val : _values)
    {
      exporter.write(getName(), "LATENCY", val);
    }
  }

	@Override
	public String getSummary() {
		DecimalFormat d = new DecimalFormat("#.##");
		double report=((double)totallatency/((double)operations));
		return "["+getName()+" AverageLatency(ms)="+d.format(report)+"]";
	}

}
