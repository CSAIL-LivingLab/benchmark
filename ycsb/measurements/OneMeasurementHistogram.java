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
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import simpledb.versioned.benchmark.ycsb.exporter.MeasurementsExporter;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 * 
 * @author cooperb
 *
 */
public class OneMeasurementHistogram extends OneMeasurement
{
	public static final String BUCKETS="histogram.buckets";
	public static final String BUCKETS_DEFAULT="100";

	//int _buckets;
	//int[] histogram;
	//int histogramoverflow;
	int operations;
	long totallatency;
	
	////keep a windowed version of these stats for printing status
	//int windowoperations;
	//long windowtotallatency;
	
	int min;
	int max;
	HashMap<Integer,int[]> returncodes;

	TreeMap<Integer, Integer> _values;

	public OneMeasurementHistogram(String name, Properties props)
	{
		super(name);
		//_buckets=Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
		//histogram=new int[_buckets];
		//histogramoverflow=0;
		_values = new TreeMap<Integer, Integer>();
		operations=0;
		totallatency=0;
		//windowoperations=0;
		//windowtotallatency=0;
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
		//if (latency>=_buckets)
		//{
		//	histogramoverflow++;
		//}
		//else
		//{
		//	histogram[latency]++;
		//}

		if (_values.containsKey(latency)) {
			_values.put(latency, _values.get(latency) + 1);
		} else {
			_values.put(latency, 1);
		}

		operations++;
		totallatency+=latency;
		//windowoperations++;
		//windowtotallatency+=latency;

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
    //for (int i=0; i<_buckets; i++)
    for (Entry<Integer, Integer> e : _values.entrySet())
    {
      //opcounter+=histogram[i];
      opcounter+=e.getValue();
      if ( (!done95th) && (((double)opcounter)/((double)operations)>=0.95) )
      {
        exporter.write(getName(), "95thPercentileLatency(ms)", e.getKey());
        done95th=true;
      }
      if (((double)opcounter)/((double)operations)>=0.99)
      {
        exporter.write(getName(), "99thPercentileLatency(ms)", e.getKey());
        break;
      }
    }

    for (Integer I : returncodes.keySet())
    {
      int[] val=returncodes.get(I);
      exporter.write(getName(), "Return="+I, val[0]);
    }     

    //for (int i=0; i<_buckets; i++)
    for (Entry<Integer, Integer> e : _values.entrySet())
    {
      exporter.write(getName(), Integer.toString(e.getKey()), e.getValue());
    }
    //exporter.write(getName(), ">"+_buckets, histogramoverflow);
  }

	@Override
	public String getSummary() {
		//if (windowoperations==0)
		//{
		//	return "";
		//}
		DecimalFormat d = new DecimalFormat("#.##");
		//double report=((double)windowtotallatency)/((double)windowoperations);
		double report=((double)totallatency/((double)operations));
		//windowtotallatency=0;
		//windowoperations=0;
		return "["+getName()+" AverageLatency(ms)="+d.format(report)+"]";
	}

}