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

package simpledb.versioned.benchmark.ycsb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import simpledb.Constants;
import simpledb.versioned.benchmark.GraphPrintWrapper;
import simpledb.versioned.benchmark.ycsb.exporter.MeasurementsExporter;
import simpledb.versioned.benchmark.ycsb.exporter.TextMeasurementsExporter;
import simpledb.versioned.benchmark.ycsb.measurements.Measurements;
import simpledb.versioned.utility.Utils;

//import org.apache.log4j.BasicConfigurator;

/**
 * A thread to periodically show the status of the experiment, to reassure you
 * that progress is being made.
 * 
 * @author cooperb
 * 
 */
class StatusThread extends Thread {

	static final Logger logger = Logger.getLogger(StatusThread.class);

	Vector<Thread> _threads;
	String _label;
	boolean _standardstatus;
	Runtime runtime;

	/**
	 * The interval for reporting status.
	 */
	public static final long sleeptime = 5000;

	public StatusThread(Vector<Thread> threads, String label, boolean standardstatus) {
		_threads = threads;
		_label = label;
		_standardstatus = standardstatus;
		runtime = Runtime.getRuntime();
	}

	/**
	 * Run and periodically report status.
	 */
	@Override
	public void run() {
		long st = System.currentTimeMillis();

		long lasten = st;
		long lasttotalops = 0;

		boolean alldone;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

		do {
			alldone = true;

			int totalops = 0;

			// terminate this thread when all the worker threads are done
			for (Thread t : _threads) {
				if (t.getState() != Thread.State.TERMINATED) {
					alldone = false;
				}

				ClientThread ct = (ClientThread) t;
				totalops += ct.getOpsDone();
			}

			long en = System.currentTimeMillis();

			long interval = en - st;
			// double throughput=1000.0*((double)totalops)/((double)interval);

			double curthroughput = 1000.0 * (((double) (totalops - lasttotalops)) / ((double) (en - lasten)));

			lasttotalops = totalops;
			lasten = en;

			DecimalFormat d = new DecimalFormat("#.##");
			String label = _label + format.format(new Date());

			logger.info(label + " Heap Usage: " + (runtime.totalMemory() - runtime.freeMemory()) / Client.MB + "MB");

			if (totalops == 0) {
				logger.info(label + " " + (interval / 1000) + " sec: " + totalops + " operations; "
						+ Measurements.getMeasurements().getSummary());
			} else {
				logger.info(label + " " + (interval / 1000) + " sec: " + totalops + " operations; "
						+ d.format(curthroughput) + " current ops/sec; " + Measurements.getMeasurements().getSummary());
			}

			if (_standardstatus) {
				if (totalops == 0) {
					logger.info(label + " " + (interval / 1000) + " sec: " + totalops + " operations; "
							+ Measurements.getMeasurements().getSummary());
				} else {
					logger.info(label + " " + (interval / 1000) + " sec: " + totalops + " operations; "
							+ d.format(curthroughput) + " current ops/sec; "
							+ Measurements.getMeasurements().getSummary());
				}
			}

			try {
				sleep(sleeptime);
			} catch (InterruptedException e) {
				// do nothing
			}

		} while (!alldone);
	}
}

/**
 * A thread for executing transactions or data inserts to the database.
 * 
 * @author cooperb
 * 
 */
class ClientThread extends Thread {

	static final Logger logger = Logger.getLogger(ClientThread.class);

	VersionDB _db;
	boolean _dotransactions;
	Workload _workload;
	double _target;

	int _opsdone;
	int _threadid;
	int _threadcount;
	Object _workloadstate;
	Properties _props;

	/**
	 * Constructor.
	 * 
	 * @param db
	 *            the DB implementation to use
	 * @param dotransactions
	 *            true to do transactions, false to insert data
	 * @param workload
	 *            the workload to use
	 * @param threadid
	 *            the id of this thread
	 * @param threadcount
	 *            the total number of threads
	 * @param props
	 *            the properties defining the experiment
	 * @param opcount
	 *            the number of operations (transactions or inserts) to do
	 * @param targetperthreadperms
	 *            target number of operations per thread per ms
	 */
	public ClientThread(VersionDB db, boolean dotransactions, Workload workload, int threadid, int threadcount,
			Properties props, double targetperthreadperms) {
		// TODO: consider removing threadcount and threadid
		_db = db;
		_dotransactions = dotransactions;
		_workload = workload;
		_opsdone = 0;
		_target = targetperthreadperms;
		_threadid = threadid;
		_threadcount = threadcount;
		_props = props;
		// System.out.println("Interval = "+interval);
	}

	public int getOpsDone() {
		return _opsdone;
	}

	@Override
	public void run() {
		try {
			_db.init();
		} catch (DBException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}

		try {
			_workloadstate = _workload.initThread(_props, _threadid, _threadcount);
		} catch (WorkloadException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}

		// spread the thread operations out so they don't all hit the DB at the
		// same time
		try {
			// GH issue 4 - throws exception if _target>1 because random.nextInt
			// argument must be >0
			// and the sleep() doesn't make sense for granularities < 1 ms
			// anyway
			if ((_target > 0) && (_target <= 1.0)) {
				sleep(Utils.random().nextInt((int) (1.0 / _target)));
			}
		} catch (InterruptedException e) {
			// do nothing.
		}

		try {
			if (_dotransactions) {
				long st = System.currentTimeMillis();

				while (!_workload.isStopRequested()) {

					Client.dropCaches();

					if (!_workload.doTransaction(_db, _workloadstate)) {
						break;
					}

					_opsdone++;

					// throttle the operations
					if (_target > 0) {
						// this is more accurate than other throttling
						// approaches we have tried,
						// like sleeping for (1/target throughput)-operation
						// latency,
						// because it smooths timing inaccuracies (from sleep()
						// taking an int,
						// current time in millis) over many operations
						while (System.currentTimeMillis() - st < (_opsdone) / _target) {
							try {
								sleep(1);
							} catch (InterruptedException e) {
								// do nothing.
							}

						}
					}
				}
			}
			// else
			// {
			// long st=System.currentTimeMillis();
			//
			// while (((_opcount == 0) || (_opsdone < _opcount)) &&
			// !_workload.isStopRequested())
			// {
			//
			// if (!_workload.doInsert(_db,_workloadstate))
			// {
			// break;
			// }
			//
			// _opsdone++;
			//
			// //throttle the operations
			// if (_target>0)
			// {
			// //this is more accurate than other throttling approaches we have
			// tried,
			// //like sleeping for (1/target throughput)-operation latency,
			// //because it smooths timing inaccuracies (from sleep() taking an
			// int,
			// //current time in millis) over many operations
			// while (System.currentTimeMillis()-st<((double)_opsdone)/_target)
			// {
			// try
			// {
			// sleep(1);
			// }
			// catch (InterruptedException e)
			// {
			// // do nothing.
			// }
			// }
			// }
			// }
			// }
		} catch (Exception e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try {
			_db.cleanup();
		} catch (DBException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}
	}
}

/**
 * Main class for executing YCSB.
 */
public class Client {
	static {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
	}

	static final Logger logger = Logger.getLogger(Client.class);

	public static final double MB = 1000000;

	public static final String BASE_DB_TEMP_DIR_PROPERTY = "db.dir";
	public static final String LOG_DIR_PROPERTY = "log.dir";

	public static final String PRINT_GRAPH_PROPERTY = "print";
	public static final String PRINT_GRAPH_PROPERTY_DEFAULT = "false";

	public static final String LOGGER_PROPERTIES_PATH_PROPERTY = "logconfig";

	public static final String WORKLOAD_PROPERTY = "workload";

	public static final String SCENARIO_PROPERTY = "scenario";

	public static final String DB_PROPERTY = "db";

	public static final String OPERATION_COUNT_PROPERTY = "operationcount";

	public static final String THREAD_COUNT_PROPERTY = "threadcount";

	/**
	 * The maximum amount of time (in seconds) for which the benchmark will be
	 * run.
	 */
	public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

	private static final String EXPERIMENT_NAME_PROPERTY = "experimentname";
	private static final String EXPERIMENT_NAME_DEFAULT = "experiment";
	private static final String EXPERIMENT_DIR_PROPERTY = "resultsdir";
	private static final String EXPERIMENT_DIR_DEFAULT = "results";
	private static final String MEASUREMENTS_FILE_NAME_PROPERTY = "measurements";
	private static final String MEASUREMENTS_FILE_NAME_DEFAULT = "measurements.txt";

	private static final String[] REQUIRED_PROPS = { DB_PROPERTY, SCENARIO_PROPERTY, WORKLOAD_PROPERTY };

	public static void usageMessage() {
		System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
		System.out.println("Options:");
		System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n"
				+ "              \"threadcount\" property using -p");
		System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n"
				+ "             be specified as the \"target\" property using -p");
		System.out.println("  -load:  run the loading phase of the workload");
		System.out.println("  -t:  run the transactions phase of the workload (default)");
		System.out.println("  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n"
				+ "              can also be specified as the \"db\" property using -p");
		System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
		System.out.println("                   be specified, and will be processed in the order specified");
		System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
		System.out.println("                  multiple properties can be specified, and override any");
		System.out.println("                  values in the propertyfile");
		System.out.println("  -s:  show status during run (default: no status)");
		System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
		System.out.println("");
		System.out.println("Required properties:");
		System.out.println("  " + WORKLOAD_PROPERTY
				+ ": the name of the workload class to use (e.g. com.yahoo.ycsb.workloads.CoreWorkload)");
		System.out.println("");
		System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out
				.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out.println(
				"use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted");
	}

	public static List<String> checkRequiredProperties(Properties props) {
		List<String> out = new ArrayList<String>();
		for (String prop : REQUIRED_PROPS) {
			if (props.getProperty(prop) == null) {
				logger.fatal("Missing property: " + WORKLOAD_PROPERTY);
				out.add(prop);
			}
		}
		return out;
	}

	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * 
	 * @throws IOException
	 *             Either failed to write to output stream or failed to close
	 *             it.
	 */
	private static void exportMeasurements(Properties props, int opcount, long runtime) throws IOException {
		MeasurementsExporter exporter = null;
		try {
			// if no destination file is provided the results will be written to
			// stdout
			OutputStream out = null;
			String exportFile = props.getProperty(MEASUREMENTS_FILE_NAME_PROPERTY);
			if (exportFile == null) {
				out = System.out;
			} else {
				out = new FileOutputStream(exportFile);
			}
			// if no exporter is provided the default text one will be used
			String exporterStr = props.getProperty("exporter", TextMeasurementsExporter.class.getName());
			try {
				exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class)
						.newInstance(out);
			} catch (Exception e) {
				System.err.println("Could not find exporter " + exporterStr + ", will use default text reporter.");
				e.printStackTrace();
				exporter = new TextMeasurementsExporter(out);
			}

			exporter.write("OVERALL", "RunTime(ms)", runtime);
			double throughput = 1000.0 * (opcount) / (runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

			Measurements.getMeasurements().exportMeasurements(exporter);
		} finally {
			if (exporter != null) {
				exporter.close();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		String dbname;
		Properties props = new Properties();
		Properties fileprops = new Properties();
		boolean dotransactions = true;
		int threadcount = 1;
		int target = 0;
		boolean status = false;
		boolean setSeed = false;
		long seed = 0;
		String label = "";

		// parse arguments
		int argindex = 0;

		if (args.length == 0) {
			usageMessage();
			System.exit(0);
		}

		while (args[argindex].startsWith("-")) {
			if (args[argindex].compareTo("-threads") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				int tcount = Integer.parseInt(args[argindex]);
				props.setProperty(THREAD_COUNT_PROPERTY, tcount + "");
				argindex++;
			} else if (args[argindex].compareTo("-target") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				int ttarget = Integer.parseInt(args[argindex]);
				props.setProperty("target", ttarget + "");
				argindex++;
			} else if (args[argindex].compareTo("-load") == 0) {
				dotransactions = false;
				argindex++;
			} else if (args[argindex].compareTo("-t") == 0) {
				dotransactions = true;
				argindex++;
			} else if (args[argindex].compareTo("-s") == 0) {
				status = true;
				argindex++;
			} else if (args[argindex].compareTo("-db") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				props.setProperty(DB_PROPERTY, args[argindex]);
				argindex++;
			} else if (args[argindex].compareTo("-sc") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				props.setProperty(SCENARIO_PROPERTY, args[argindex]);
				argindex++;
			} else if (args[argindex].compareTo("-l") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				label = args[argindex];
				argindex++;
			} else if (args[argindex].compareTo("-seed") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				seed = Long.parseLong(args[argindex]);
				setSeed = true;
				argindex++;
			} else if (args[argindex].compareTo("-P") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				String propfile = args[argindex];
				argindex++;

				Properties myfileprops = new Properties();
				try {
					myfileprops.load(new FileInputStream(propfile));
				} catch (IOException e) {
					System.out.println(e.getMessage());
					System.exit(0);
				}

				// Issue #5 - remove call to stringPropertyNames to make
				// compilable under Java 1.5
				for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements();) {
					String prop = (String) e.nextElement();

					fileprops.setProperty(prop, myfileprops.getProperty(prop));
				}
			} else if (args[argindex].compareTo("-name") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				String experimentName = args[argindex];
				fileprops.setProperty(EXPERIMENT_NAME_PROPERTY, experimentName);
				argindex++;
			} else if (args[argindex].compareTo("-dir") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				String dir = args[argindex];
				fileprops.setProperty(EXPERIMENT_DIR_PROPERTY, dir);
				argindex++;
			} else if (args[argindex].compareTo("-p") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				int eq = args[argindex].indexOf('=');
				if (eq < 0) {
					usageMessage();
					System.exit(0);
				}

				String name = args[argindex].substring(0, eq);
				String value = args[argindex].substring(eq + 1);
				props.put(name, value);
				argindex++;
			} else {
				System.out.println("Unknown option " + args[argindex]);
				usageMessage();
				System.exit(0);
			}

			if (argindex >= args.length) {
				break;
			}
		}

		if (argindex != args.length) {
			usageMessage();
			System.exit(0);
		}

		// overwrite file properties with properties from the command line

		// Issue #5 - remove call to stringPropertyNames to make compilable
		// under Java 1.5
		for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
			String prop = (String) e.nextElement();

			fileprops.setProperty(prop, props.getProperty(prop));
		}

		props = fileprops;

		List<String> missingProps = checkRequiredProperties(props);

		if (missingProps.size() != 0) {
			System.err.println("Properties are missing: " + missingProps + ". Terminating...");
			System.exit(-1);
		}

		// configure directory stuff
		String experimentName = props.getProperty(EXPERIMENT_NAME_PROPERTY, EXPERIMENT_NAME_DEFAULT);
		String resultsDirName = props.getProperty(EXPERIMENT_DIR_PROPERTY, EXPERIMENT_DIR_DEFAULT);

		File resultDir = new File(resultsDirName);
		if (!resultDir.exists()) {
			resultDir.mkdir();
		}

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
		String effectiveResultsDirName = resultsDirName + File.separator + experimentName
				+ dateFormat.format(new Date());

		File effectiveResultsDir = new File(effectiveResultsDirName);
		effectiveResultsDir.delete();
		effectiveResultsDir.mkdir();

		String effectivrResultsFile = effectiveResultsDir.getAbsolutePath() + File.separator
				+ props.getProperty(MEASUREMENTS_FILE_NAME_PROPERTY, MEASUREMENTS_FILE_NAME_DEFAULT);
		props.put(MEASUREMENTS_FILE_NAME_PROPERTY, effectivrResultsFile);

		// create temp directory
		String tempDirName = effectiveResultsDirName + File.separator + "temp";
		File tempDir = new File(tempDirName);
		tempDir.mkdir();

		// set temp file directory
		System.setProperty(BASE_DB_TEMP_DIR_PROPERTY, tempDir.getAbsolutePath());
		System.setProperty(LOG_DIR_PROPERTY, effectiveResultsDir.getAbsolutePath());

		// set logger
		String loggerPropertiesPath = props.getProperty(LOGGER_PROPERTIES_PATH_PROPERTY);
		if (loggerPropertiesPath != null) {
			DOMConfigurator.configure(loggerPropertiesPath);
			System.out.println("Logging mode enabled, see log file for output!");
		}

		logger.info("***** Versioned Benchmark Client 1.0 *****");
		logger.info("Max JVM Memory (MB): " + Runtime.getRuntime().maxMemory() / MB);
		logger.info("Results dir is: " + effectiveResultsDir.getAbsolutePath());
		logger.info("Temp dir is: " + tempDir.getAbsolutePath());
		System.out.print("Command line:");
		String argsStr = "";
		for (int i = 0; i < args.length; i++) {
			argsStr += args[i];
		}

		logger.info("args were: " + argsStr);

		if (setSeed) {
			logger.info("************ Seed was set to " + seed + " ************");
		} else {
			seed = System.currentTimeMillis();
			logger.warn("************ Seed was derived from time ************");
		}
		System.setProperty(Constants.RNG_SEED, String.valueOf(seed));

		long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));
		logger.info("Max Execution Time: " + maxExecutionTime);

		// get number of threads, target and db
		threadcount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
		logger.info("Thread Count: " + threadcount);

		if (threadcount != 1) {
			logger.fatal("More than 1 thread requested, this is not supported. Terminating...");
			throw new IllegalArgumentException("Too many thread! Bechmark is not multi-threaded yet! Terminating.");
		}

		dbname = props.getProperty(DB_PROPERTY);
		logger.info("Using DB: " + dbname);
		target = Integer.parseInt(props.getProperty("target", "0"));

		// compute the target throughput
		double targetperthreadperms = -1;
		if (target > 0) {
			double targetperthread = ((double) target) / ((double) threadcount);
			targetperthreadperms = targetperthread / 1000.0;
		}

		logger.info("Loading...");

		// show a warning message that creating the workload is taking a while
		// but only do so if it is taking longer than 2 seconds
		// (showing the message right away if the setup wasn't taking very long
		// was confusing people)
		Thread warningthread = new Thread() {
			@Override
			public void run() {
				try {
					sleep(2000);
				} catch (InterruptedException e) {
					return;
				}
				logger.warn(" (might take a few minutes for large data sets)");
			}
		};

		warningthread.start();

		// set up measurements
		Measurements.setProperties(props);

		// load setup
		ClassLoader classLoader = Client.class.getClassLoader();

		logger.info("Loading benchmark classes...");

		// load Scenario
		Scenario scenario = null;

		try {
			Class scenarioclass = classLoader.loadClass(props.getProperty(SCENARIO_PROPERTY));

			scenario = (Scenario) scenarioclass.newInstance();
		} catch (Exception e) {
			logger.fatal("Failed to load scenario class!", e.getCause());
			System.exit(-1);
		}

		// load the workload
		Workload workload = null;

		try {
			Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));
			workload = (Workload) workloadclass.newInstance();
		} catch (Exception e) {
			logger.fatal("Failed to load workload class!", e);
			System.exit(-1);
		}

		// set database
		VersionDB db = null;
		try {
			db = DBFactory.newDB(dbname, props);
			// db.setMeasurements(Measurements.getMeasurements());
		} catch (UnknownDBException e) {
			logger.fatal("Failed to load database class!", e);
			System.exit(-1);
		}

		boolean printGraph = Boolean
				.parseBoolean(props.getProperty(PRINT_GRAPH_PROPERTY, PRINT_GRAPH_PROPERTY_DEFAULT));

		if (printGraph) {
			logger.info("Print graph enabled!");
			db = new GraphPrintWrapper(db);
			((GraphPrintWrapper) db).printGraph();
		}

		logger.info("Initializing Scenario!");

		try {
			scenario.init(props, db);
		} catch (Exception e) {
			logger.fatal("Failed to init scenario", e);
			cleanUp(scenario);
			System.exit(-1);
		}

		if (printGraph) {
			logger.info(db.getVersionMetaDataStringRep(scenario.getVersionTableEntry().tableName));
		}

		try {
			db.prepareForWorkload(scenario.getVersionTableEntry().tableName);
		} catch (Exception e) {
			logger.fatal("Failed to repack!", e);
			cleanUp(scenario, workload);
			System.exit(-1);
		}

		try {
			db.flushAllPages();
			db.force(scenario.getVersionTableEntry().tableName);
		} catch (IOException e1) {
			logger.warn("Failed to flush and close files!", e1);
		}

		logger.info("Total Size of table and meta data stores (MB): "
				+ db.getSize(scenario.getVersionTableEntry().tableName) / MB);

		logger.info("Initializing Workload!");
		try {
			workload.init(props, scenario);
		} catch (Exception e) {
			logger.fatal("Failed to init workload", e);
			cleanUp(scenario, workload);
			System.exit(-1);
		}

		warningthread.interrupt();

		// run the workload

		logger.info("Starting experiment!");

		Vector<Thread> threads = new Vector<Thread>();

		for (int threadid = 0; threadid < threadcount; threadid++) {
			// did this so that graph printing will work correctly,
			// this won't work in a multi-threaded environment
			// try
			// {
			// db=DBFactory.newDB(dbname,props);
			// }
			// catch (UnknownDBException e)
			// {
			// System.out.println("Unknown DB "+dbname);
			// System.exit(0);
			// }

			Thread t = new ClientThread(db, dotransactions, workload, threadid, threadcount, props,
					targetperthreadperms);

			threads.add(t);
		}

		StatusThread statusthread = null;

		if (status) {
			boolean standardstatus = false;
			if (props.getProperty("measurementtype", "").compareTo("timeseries") == 0) {
				standardstatus = true;
			}
			statusthread = new StatusThread(threads, label, standardstatus);
			statusthread.start();
		}

		long st = System.currentTimeMillis();

		for (Thread t : threads) {
			t.start();
		}
		logger.info("Started threads! Benchmark Started!");

		Thread terminator = null;

		if (maxExecutionTime > 0) {
			terminator = new TerminatorThread(maxExecutionTime, threads, workload);
			terminator.start();
		}

		int opsDone = 0;

		for (Thread t : threads) {
			try {
				t.join();
				opsDone += ((ClientThread) t).getOpsDone();
			} catch (Exception e) {
				System.err.println("Experiment failed! Fatal error!");
				e.printStackTrace();
			}
		}

		logger.info("Experiment Complete!");

		long en = System.currentTimeMillis();

		if (terminator != null && !terminator.isInterrupted()) {
			terminator.interrupt();
		}

		if (status) {
			statusthread.interrupt();
		}

		logger.info("Exporting measurements...");

		try {
			exportMeasurements(props, opsDone, en - st);
		} catch (IOException e) {
			logger.fatal("Could not export measurements, error: " + e.getMessage(), e);
		}

		logger.info("Cleaning up...");

		cleanUp(scenario, workload);
		tempDir.delete();

		if (printGraph) {
			logger.info("Graphing enabled: Waiting for graph.");
			((GraphPrintWrapper) db).waitForGraph();
		}

		// needed to close all log4j appenders
		LogManager.shutdown();

		System.exit(0);
	}

	private static void cleanUp(Cleanupable... dataToClean) {
		for (Cleanupable cleanUpObj : dataToClean) {
			try {
				cleanUpObj.cleanup();
			} catch (DBException e) {
				logger.warn("Clean up failed!", e);
			}
		}
	}

	static String executeCommand(String command) {
		StringBuffer output = new StringBuffer();

		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
		} catch (Exception e) {
			logger.debug("Could not drop caches! " + e);
		}

		return output.toString();
	}

	public static void dropCaches() {
		logger.debug(
				"ClientThread: Workload transaction commencing. Dropping caches and performing a full collection.");
		Client.executeCommand("drop_caches");
		System.gc();
	}
}
