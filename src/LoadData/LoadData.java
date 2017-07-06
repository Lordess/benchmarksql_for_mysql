/*
 * Copyright (C) 2004-2014, Denis Lussier
 *
 * LoadData - Load Sample Data directly into database tables or create CSV files for
 *            each table that can then be bulk loaded (again & again & again ...)  :-)
 *
 *    Two optional parameter sets for the command line:
 *
 *                 numWarehouses=9999
 *
 *                 fileLocation=/temp/csv/
 *
 *    "numWarehouses" defaults to "1" and when "fileLocation" is omitted the generated
 *    data is loaded into the database tables directly.
 ***************************************************************************************
 */

import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.*;
import java.lang.Integer;

/*
 * 每个 Warehouse 的数据量，其大小约为 76823.04KB，可以有小量的变化，因为测试过程中将会插入或删除现有记录。可以根据每个Warehouse的数据量，计算测试过程中的数据总量。
 计算公式为：数据总量（KB）≈ Warehouse个数*76823.04KB
 以10个Warehouse的数据量为例计算其数据总量大小约为：768230.4KB
 */
public class LoadData implements jTPCCConfig {

	// *********** JDBC specific variables ***********************
	private static Connection conn = null;
	private static Connection[] conns = null;

	private static java.sql.Timestamp sysdate = null;

	// ********** general vars **********************************
	private static java.util.Date now = null;
	private static java.util.Date startDate = null;
	private static java.util.Date endDate = null;

	private static Random gen;
	// private static String dbType;
	private static int numWarehouses = 0;
	private static String fileLocation = "";
	private static boolean outputFiles = false;
	private static PrintWriter out = null;
	private static long lastTimeMS = 0;

	// 并行度：同时起多少个线程执行并行加载
	private static int parallella = 1;
	private static ExecutorService threadPool = null;

	// private static List<Future<Integer>> fts = new
	// ArrayList<Future<Integer>>();
	//private static java.util.concurrent.BlockingQueue<Future<Integer>> fts = new java.util.concurrent.LinkedBlockingQueue<Future<Integer>>();
	private static java.util.concurrent.BlockingQueue<Future<Integer>> fts = null;
	
	private static int warehouses_initvalue=1;

	public static void main(String[] args) throws Exception {

		System.out.println("Starting BenchmarkSQL LoadData");

		System.out
				.println("----------------- Initialization -------------------");

		numWarehouses = configWhseCount;
		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i]);
			String str = args[i];
			if (str.toLowerCase().startsWith("numwarehouses")) {
				String val = args[i + 1];
				numWarehouses = Integer.parseInt(val);
			}

			if (str.toLowerCase().startsWith("warehouses_initvalue")) {
				String val = args[i + 1];
				warehouses_initvalue = Integer.parseInt(val);
			}

			if (str.toLowerCase().startsWith("filelocation")) {
				fileLocation = args[i + 1];
				outputFiles = true;
			}

			if (str.toLowerCase().startsWith("parallella")) {
				String val = args[i + 1];
				parallella = Integer.parseInt(val);
			}
		}

		threadPool = Executors.newFixedThreadPool(parallella);
		fts = new java.util.concurrent.ArrayBlockingQueue<Future<Integer>>(parallella * 5);
		// threadPool = new ThreadPoolExecutor(parallella, 30000, 180,
		// TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		if (outputFiles == false) {
			initJDBC();
		}

		// seed the random number generator
		gen = new Random(System.currentTimeMillis());

		// ######################### MAINLINE
		// ######################################
		startDate = new java.util.Date();
		System.out.println("");
		System.out.println("------------- LoadData StartTime = " + startDate
				+ "-------------");

		long startTimeMS = new java.util.Date().getTime();
		lastTimeMS = startTimeMS;

		System.out.println("");
		long totalRows = loadWhse(numWarehouses);
		System.out.println("");
		if(warehouses_initvalue==1){
			//只有当从1开始时，才需要重建Item表的数据，否则，这个数据是固定的，无需再创建。
			totalRows += loadItem(configItemCount);
			System.out.println("");
			ai = new AtomicInteger(1);
		}else{

			//ai = new AtomicInteger( (warehouses_initvalue -1) * configDistPerWhse * configCustPerDist + 1);
			ai = new AtomicInteger( getMaxHistId() + 1 );
		}
		totalRows += loadStock(numWarehouses, configItemCount);
		System.out.println("");
		totalRows += loadDist(numWarehouses, configDistPerWhse);
		System.out.println("");
		totalRows += loadCust(numWarehouses, configDistPerWhse,
				configCustPerDist);
		System.out.println("");
		totalRows += loadOrder(numWarehouses, configDistPerWhse,
				configCustPerDist);
		// 必须shutdown，否则线程池中的线程会阻塞，main进程无法完全退出。
		((ThreadPoolExecutor) threadPool).shutdown();
		long runTimeMS = (new java.util.Date().getTime()) + 1 - startTimeMS;
		endDate = new java.util.Date();
		System.out.println("");
		System.out
				.println("------------- LoadJDBC Statistics --------------------");
		System.out.println("     Start Time = " + startDate);
		System.out.println("       End Time = " + endDate);
		System.out.println("       Run Time = " + (int) runTimeMS / 1000
				+ " Seconds");
		System.out.println("    Rows Loaded = " + totalRows + " Rows");
		System.out.println("Rows Per Second = "
				+ ((totalRows * 10000) / (runTimeMS * 10)) + " Rows/Sec");
		System.out
				.println("------------------------------------------------------");

		// exit Cleanly
		try {
			if (outputFiles == false) {
				if (conn != null)
					conn.close();
				for (int i = 0; i < parallella; i++) {
					conns[i].close();
				}
			}
		} catch (SQLException se) {
			se.printStackTrace();
		} // end try

	} // end main

	static int getMaxHistId(){
		int max_id=0;
		try {
			PreparedStatement histPrepStmt = null;
			now = new java.util.Date();
			System.out.println("Get Max History ID "
					+ now + " ...");
			
			histPrepStmt = conn
					.prepareStatement("select max(hist_id) as max_id from benchmarksql.history ");
		
					
			ResultSet rs = histPrepStmt.executeQuery();
			if (!rs.next()) {
				System.out.println("Max History ID not found! ");
			}else{
				max_id=rs.getInt("max_id");
			}

					
			rs.close();
			rs = null;
			histPrepStmt.close();
			histPrepStmt = null;			
				
			now = new java.util.Date();

			long tmpTime = new java.util.Date().getTime();
			System.out.println("Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000));
			lastTimeMS = tmpTime;
			System.out.println("Max History ID " + (max_id));

		} catch (SQLException se) {
			while (se != null) {
				System.out.println(se.getMessage());
				se = se.getNextException();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		return max_id;
	}

	static void transRollback() {
		transRollback(conn);
	}

	static void transRollback(Connection con) {
		if (outputFiles == false) {
			try {
				con.rollback();
			} catch (SQLException se) {
				System.out.println(se.getMessage());
			}
		} else {
			out.close();
		}
	}

	static void transCommit() {
		transCommit(conn);
	}

	static void transCommit(Connection con) {
		if (outputFiles == false) {
			try {
				con.commit();
			} catch (SQLException se) {
				System.out.println(se.getMessage());
				transRollback(con);
			}
		} else {
			out.close();
		}
	}

	static void initJDBC() {

		try {

			// load the ini file
			Properties ini = new Properties();
			ini.load(new FileInputStream(System.getProperty("prop")));

			// display the values we need
			System.out.println("driver=" + ini.getProperty("driver"));
			System.out.println("conn=" + ini.getProperty("conn"));
			System.out.println("user=" + ini.getProperty("user"));
			System.out.println("password=******");

			// Register jdbcDriver
			Class.forName(ini.getProperty("driver"));

			// make connection
			conn = DriverManager.getConnection(ini.getProperty("conn"),
					ini.getProperty("user"), ini.getProperty("password"));
			conn.setAutoCommit(false);

			conns = new Connection[parallella];
			for (int i = 0; i < parallella; i++) {
				conns[i] = DriverManager.getConnection(ini.getProperty("conn"),
						ini.getProperty("user"), ini.getProperty("password"));
				conns[i].setAutoCommit(false);
			}

			// Create Statement
			// stmt = conn.createStatement();

			

			

			

		} catch (SQLException se) {
			System.out.println(se.getMessage());
			transRollback();

		} catch (Exception e) {
			e.printStackTrace();
			transRollback();

		} // end try

	} // end initJDBC()

	static int loadItem(int itemKount) {

		int k = 0;
		int t = 0;
		int randPct = 0;
		int len = 0;
		int startORIGINAL = 0;

		try {
			
			PreparedStatement itemPrepStmt = null;

			now = new java.util.Date();
			t = itemKount;
			System.out.println("Start Item Load for " + t + " Items @ " + now
					+ " ...");

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "item.csv"));
				System.out.println("Writing Item file to: " + fileLocation
						+ "item.csv");
			}else{
				itemPrepStmt = conn
						.prepareStatement("INSERT INTO benchmarksql.item "
								+ " (i_id, i_name, i_price, i_data, i_im_id) "
								+ "VALUES (?, ?, ?, ?, ?)");
			}

			Item item = new Item();

			for (int i = 1; i <= itemKount; i++) {

				item.i_id = i;
				item.i_name = jTPCCUtil.randomStr(jTPCCUtil.randomNumber(14,
						24, gen));
				item.i_price = (float) (jTPCCUtil.randomNumber(100, 10000, gen) / 100.0);

				// i_data
				randPct = jTPCCUtil.randomNumber(1, 100, gen);
				len = jTPCCUtil.randomNumber(26, 50, gen);
				if (randPct > 10) {
					// 90% of time i_data isa random string of length [26 .. 50]
					item.i_data = jTPCCUtil.randomStr(len);
				} else {
					// 10% of time i_data has "ORIGINAL" crammed somewhere in
					// middle
					startORIGINAL = jTPCCUtil.randomNumber(2, (len - 8), gen);
					item.i_data = jTPCCUtil.randomStr(startORIGINAL - 1)
							+ "ORIGINAL"
							+ jTPCCUtil.randomStr(len - startORIGINAL - 9);
				}

				item.i_im_id = jTPCCUtil.randomNumber(1, 10000, gen);

				k++;

				if (outputFiles == false) {
					itemPrepStmt.setLong(1, item.i_id);
					itemPrepStmt.setString(2, item.i_name);
					itemPrepStmt.setDouble(3, item.i_price);
					itemPrepStmt.setString(4, item.i_data);
					itemPrepStmt.setLong(5, item.i_im_id);
					itemPrepStmt.addBatch();

					if ((k % configCommitCount) == 0) {
						long tmpTime = new java.util.Date().getTime();
						String etStr = "  Elasped Time(ms): "
								+ ((tmpTime - lastTimeMS) / 1000.000)
								+ "                    ";
						System.out.println(etStr.substring(0, 30)
								+ "  Writing record " + k + " of " + t);
						lastTimeMS = tmpTime;
						itemPrepStmt.executeBatch();
						itemPrepStmt.clearBatch();
						transCommit();
					}
				} else {
					String str = "";
					str = str + item.i_id + ",";
					str = str + item.i_name + ",";
					str = str + item.i_price + ",";
					str = str + item.i_data + ",";
					str = str + item.i_im_id;
					out.println(str);

					if ((k % configCommitCount) == 0) {
						long tmpTime = new java.util.Date().getTime();
						String etStr = "  Elasped Time(ms): "
								+ ((tmpTime - lastTimeMS) / 1000.000)
								+ "                    ";
						System.out.println(etStr.substring(0, 30)
								+ "  Writing record " + k + " of " + t);
						lastTimeMS = tmpTime;
					}
				}

			} // end for

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			System.out.println(etStr.substring(0, 30)
					+ "  Writing final records " + k + " of " + t);
			lastTimeMS = tmpTime;

			if (outputFiles == false) {
				itemPrepStmt.executeBatch();
			}
			transCommit();
			itemPrepStmt.close();
			itemPrepStmt=null;
			now = new java.util.Date();
			System.out.println("End Item Load @  " + now);

		} catch (SQLException se) {
			System.out.println(se.getMessage());
			transRollback();
		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
		}

		return (k);

	} // end loadItem()

	static int loadWhse(int whseKount) {
		int k=0;
		try {
			PreparedStatement whsePrepStmt = null;
			now = new java.util.Date();
			System.out.println("Start Whse Load for " + whseKount + " Whses @ "
					+ now + " ...");

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "warehouse.csv"));
				System.out.println("Writing Warehouse file to: " + fileLocation
						+ "warehouse.csv");
			}else{
				whsePrepStmt = conn
						.prepareStatement("INSERT INTO benchmarksql.warehouse "
								+ " (w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip) "
								+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
			}

			Warehouse warehouse = new Warehouse();
			for (int i = warehouses_initvalue; i <= whseKount; i++) {

				warehouse.w_id = i;
				warehouse.w_ytd = 300000;

				// random within [0.0000 .. 0.2000]
				warehouse.w_tax = (float) ((jTPCCUtil
						.randomNumber(0, 2000, gen)) / 10000.0);

				warehouse.w_name = jTPCCUtil.randomStr(jTPCCUtil.randomNumber(
						6, 10, gen));
				warehouse.w_street_1 = jTPCCUtil.randomStr(jTPCCUtil
						.randomNumber(10, 20, gen));
				warehouse.w_street_2 = jTPCCUtil.randomStr(jTPCCUtil
						.randomNumber(10, 20, gen));
				warehouse.w_city = jTPCCUtil.randomStr(jTPCCUtil.randomNumber(
						10, 20, gen));
				warehouse.w_state = jTPCCUtil.randomStr(3).toUpperCase();
				warehouse.w_zip = "123456789";

				k++;
				if (outputFiles == false) {
					whsePrepStmt.setLong(1, warehouse.w_id);
					whsePrepStmt.setDouble(2, warehouse.w_ytd);
					whsePrepStmt.setDouble(3, warehouse.w_tax);
					whsePrepStmt.setString(4, warehouse.w_name);
					whsePrepStmt.setString(5, warehouse.w_street_1);
					whsePrepStmt.setString(6, warehouse.w_street_2);
					whsePrepStmt.setString(7, warehouse.w_city);
					whsePrepStmt.setString(8, warehouse.w_state);
					whsePrepStmt.setString(9, warehouse.w_zip);
					whsePrepStmt.addBatch();
					
					if ((k % configCommitCount) == 0) {
						boolean commit = false;
						whsePrepStmt.executeBatch();
						whsePrepStmt.clearBatch();
						
						commit = true;
						transCommit(conn);
						
						long tmpTime = new java.util.Date()
								.getTime();
						String etStr = "  Elasped Time(ms): "
								+ ((tmpTime - lastTimeMS) / 1000.000)
								+ "                    ";
						
						lastTimeMS = tmpTime;
					}
				} else {
					String str = "";
					str = str + warehouse.w_id + ",";
					str = str + warehouse.w_ytd + ",";
					str = str + warehouse.w_tax + ",";
					str = str + warehouse.w_name + ",";
					str = str + warehouse.w_street_1 + ",";
					str = str + warehouse.w_street_2 + ",";
					str = str + warehouse.w_city + ",";
					str = str + warehouse.w_state + ",";
					str = str + warehouse.w_zip;
					out.println(str);
				}

			} // end for

				if (outputFiles == false) {
					whsePrepStmt.executeBatch();
				}
				transCommit(conn);
				whsePrepStmt.close();
				whsePrepStmt = null;
			now = new java.util.Date();

			long tmpTime = new java.util.Date().getTime();
			System.out.println("Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000));
			lastTimeMS = tmpTime;
			System.out.println("End Whse Load @  " + now);

		} catch (SQLException se) {
			while (se != null) {
				System.out.println(se.getMessage());
				se = se.getNextException();
			}
			transRollback();
		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
		}

		return (whseKount - warehouses_initvalue + 1);

	} // end loadWhse()

	static int loadStock(final int whseKount, final int itemKount) {

		int kk = 0;

		final int t = ( (whseKount - warehouses_initvalue + 1) * itemKount);

		now = new java.util.Date();

		System.out.println("Start Stock Load for " + t + " units @ " + now
				+ " ...");

		if (outputFiles == true) {

			try {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "stock.csv"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			System.out.println("Writing Stock file to: " + fileLocation
					+ "stock.csv");
		}

		Thread asCall = new Thread() {
			public void run() {
				for (int iw = warehouses_initvalue; iw <= whseKount; iw++) {
					final int w = iw;
					int c = w % parallella;
					final Connection con;
					if (!outputFiles)
						con = conns[c];
					else
						con = null;
					Callable<Integer> runstk = new Callable<Integer>() {
						public Integer call() {
							int k = 0;
							PreparedStatement stckPrepStmt = null;

							try {
								if (!outputFiles)
									stckPrepStmt = con
											.prepareStatement("INSERT INTO benchmarksql.stock "
													+ " (s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, "
													+ "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
													+ "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10) "
													+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

								Stock stock = new Stock();
								stock.s_w_id = w;
								stock.s_ytd = 0;
								stock.s_order_cnt = 0;
								stock.s_remote_cnt = 0;
								for (int i = 1; i <= itemKount; i++) {

									stock.s_i_id = i;

									stock.s_quantity = jTPCCUtil.randomNumber(
											10, 100, gen);

									// s_data
									int randPct = 0;
									int len = 0;
									int startORIGINAL = 0;
									randPct = jTPCCUtil.randomNumber(1, 100,
											gen);
									len = jTPCCUtil.randomNumber(26, 50, gen);
									if (randPct > 10) {
										// 90% of time i_data isa random string
										// of
										// length [26 ..
										// 50]
										stock.s_data = jTPCCUtil.randomStr(len);
									} else {
										// 10% of time i_data has "ORIGINAL"
										// crammed
										// somewhere
										// in middle
										startORIGINAL = jTPCCUtil.randomNumber(
												2, (len - 8), gen);
										stock.s_data = jTPCCUtil
												.randomStr(startORIGINAL - 9)
												+ "ORIGINAL"
												+ jTPCCUtil.randomStr(len
														- startORIGINAL - 9);
									}

									stock.s_dist_01 = jTPCCUtil.randomStr(24);
									stock.s_dist_02 = jTPCCUtil.randomStr(24);
									stock.s_dist_03 = jTPCCUtil.randomStr(24);
									stock.s_dist_04 = jTPCCUtil.randomStr(24);
									stock.s_dist_05 = jTPCCUtil.randomStr(24);
									stock.s_dist_06 = jTPCCUtil.randomStr(24);
									stock.s_dist_07 = jTPCCUtil.randomStr(24);
									stock.s_dist_08 = jTPCCUtil.randomStr(24);
									stock.s_dist_09 = jTPCCUtil.randomStr(24);
									stock.s_dist_10 = jTPCCUtil.randomStr(24);

									k++;
									if (outputFiles == false) {
										stckPrepStmt.setLong(1, stock.s_i_id);
										stckPrepStmt.setLong(2, stock.s_w_id);
										stckPrepStmt.setDouble(3,
												stock.s_quantity);
										stckPrepStmt.setDouble(4, stock.s_ytd);
										stckPrepStmt.setLong(5,
												stock.s_order_cnt);
										stckPrepStmt.setLong(6,
												stock.s_remote_cnt);
										stckPrepStmt.setString(7, stock.s_data);
										stckPrepStmt.setString(8,
												stock.s_dist_01);
										stckPrepStmt.setString(9,
												stock.s_dist_02);
										stckPrepStmt.setString(10,
												stock.s_dist_03);
										stckPrepStmt.setString(11,
												stock.s_dist_04);
										stckPrepStmt.setString(12,
												stock.s_dist_05);
										stckPrepStmt.setString(13,
												stock.s_dist_06);
										stckPrepStmt.setString(14,
												stock.s_dist_07);
										stckPrepStmt.setString(15,
												stock.s_dist_08);
										stckPrepStmt.setString(16,
												stock.s_dist_09);
										stckPrepStmt.setString(17,
												stock.s_dist_10);
										stckPrepStmt.addBatch();
										if ((k % configCommitCount) == 0) {
											boolean commit = false;
											stckPrepStmt.executeBatch();
											stckPrepStmt.clearBatch();
											// if((k /configCommitCount) % 4
											// ==0){
											commit = true;
											transCommit(con);
											// }
											long tmpTime = new java.util.Date()
													.getTime();
											String etStr = "  Elasped Time(ms): "
													+ ((tmpTime - lastTimeMS) / 1000.000)
													+ "                    ";
											System.out
													.println(etStr.substring(0,
															30)
															+ "  Writing record "
															+ (w - 1)
															+ ": "
															+ k
															+ " of "
															+ t
															+ (commit ? " commit"
																	: ""));
											lastTimeMS = tmpTime;

										}
									} else {
										StringBuffer str = new StringBuffer(240);
										str.append(stock.s_i_id).append(",")
												.append(stock.s_w_id)
												.append(",")
												.append(stock.s_quantity)
												.append(",")
												.append(stock.s_ytd)
												.append(",")
												.append(stock.s_order_cnt)
												.append(",")
												.append(stock.s_remote_cnt)
												.append(",")
												.append(stock.s_data)
												.append(",")
												.append(stock.s_dist_01)
												.append(",")
												.append(stock.s_dist_02)
												.append(",")
												.append(stock.s_dist_03)
												.append(",")
												.append(stock.s_dist_04)
												.append(",")
												.append(stock.s_dist_05)
												.append(",")
												.append(stock.s_dist_06)
												.append(",")
												.append(stock.s_dist_07)
												.append(",")
												.append(stock.s_dist_08)
												.append(",")
												.append(stock.s_dist_09)
												.append(",")
												.append(stock.s_dist_10);
										out.println(str);

										if ((k % configCommitCount) == 0) {
											long tmpTime = new java.util.Date()
													.getTime();
											String etStr = "  Elasped Time(ms): "
													+ ((tmpTime - lastTimeMS) / 1000.000)
													+ "                    ";
											System.out.println(etStr.substring(
													0, 30)
													+ "  Writing record "
													+ (w - 1)
													+ ": "
													+ k
													+ " of " + t);
											lastTimeMS = tmpTime;
										}
									}

								} // end for [i]

								if (outputFiles == false) {

									stckPrepStmt.executeBatch();
									transCommit(con);
									stckPrepStmt.clearBatch();
									stckPrepStmt.close();
									stckPrepStmt=null;
								}
								
							} catch (SQLException se) {

								while (se != null) {
									System.out.println(se.getMessage());
									se = se.getNextException();
								}
								transRollback(con);

							} catch (Exception e) {
								e.printStackTrace();
								transRollback(con);
							} 
							return k;
						}// end of call()
					};
					// FutureTask<Integer> ft = new FutureTask<Integer>(runstk);
					// threadPool.execute(ft);
					Future<Integer> ft = threadPool.submit(runstk);
					System.out.println("Parallel load Stock "
							+ (new java.util.Date()));
					//add 将指定的元素添加到此队列中（如果立即可行），在成功时返回 true，其他情况则抛出 IllegalStateException。
					//fts.add(ft);
					//offer 将指定的元素插入此队列中，如果没有可用空间，将等待指定的等待时间（如果有必要）。
					try {
						fts.offer(ft, 10, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} // end for [w]
			}
		};
		asCall.start();

		try {
			System.out.println("Waiting Parallel load Stock "
					+ (new java.util.Date()));
			// for (Future<Integer> ft : fts) {
			// kk += ft.get();
			// }
			// fts.clear();
			Future<Integer> ft = fts.take();
			while (ft != null) {
				kk += ft.get();
				ft=null;
				ft = fts.poll();
			}

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long tmpTime = new java.util.Date().getTime();
		String etStr = "  Elasped Time(ms): "
				+ ((tmpTime - lastTimeMS) / 1000.000) + "                    ";
		System.out.println(etStr.substring(0, 30) + "  Writing final records "
				+ kk + " of " + t);
		lastTimeMS = tmpTime;

		now = new java.util.Date();
		System.out.println("End Stock Load @  " + now);

		return (kk);

	} // end loadStock()

	static int loadDist(int whseKount, int distWhseKount) {

		int k = 0;
		int t = 0;

		try {
			PreparedStatement distPrepStmt =null;
			now = new java.util.Date();

			if (outputFiles == true) {
				out = new PrintWriter(new FileOutputStream(fileLocation
						+ "district.csv"));
				System.out.println("Writing District file to: " + fileLocation
						+ "district.csv");
			}else{
				distPrepStmt = conn
						.prepareStatement("INSERT INTO benchmarksql.district "
								+ " (d_id, d_w_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip) "
								+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			}

			District district = new District();

			t = ( (whseKount - warehouses_initvalue + 1)* distWhseKount);
			System.out.println("Start District Data for " + t + " Dists @ "
					+ now + " ...");

			for (int w = warehouses_initvalue; w <= whseKount; w++) {

				for (int d = 1; d <= distWhseKount; d++) {

					district.d_id = d;
					district.d_w_id = w;
					district.d_ytd = 30000;

					// random within [0.0000 .. 0.2000]
					district.d_tax = (float) ((jTPCCUtil.randomNumber(0, 2000,
							gen)) / 10000.0);

					district.d_next_o_id = 3001;
					district.d_name = jTPCCUtil.randomStr(jTPCCUtil
							.randomNumber(6, 10, gen));
					district.d_street_1 = jTPCCUtil.randomStr(jTPCCUtil
							.randomNumber(10, 20, gen));
					district.d_street_2 = jTPCCUtil.randomStr(jTPCCUtil
							.randomNumber(10, 20, gen));
					district.d_city = jTPCCUtil.randomStr(jTPCCUtil
							.randomNumber(10, 20, gen));
					district.d_state = jTPCCUtil.randomStr(3).toUpperCase();
					district.d_zip = "123456789";

					k++;
					if (outputFiles == false) {
						distPrepStmt.setLong(1, district.d_id);
						distPrepStmt.setLong(2, district.d_w_id);
						distPrepStmt.setDouble(3, district.d_ytd);
						distPrepStmt.setDouble(4, district.d_tax);
						distPrepStmt.setLong(5, district.d_next_o_id);
						distPrepStmt.setString(6, district.d_name);
						distPrepStmt.setString(7, district.d_street_1);
						distPrepStmt.setString(8, district.d_street_2);
						distPrepStmt.setString(9, district.d_city);
						distPrepStmt.setString(10, district.d_state);
						distPrepStmt.setString(11, district.d_zip);
						distPrepStmt.addBatch();
						if ((k % configCommitCount) == 0) {
							boolean commit = false;
							distPrepStmt.executeBatch();
							distPrepStmt.clearBatch();
							
							commit = true;
							transCommit(conn);
							
							long tmpTime = new java.util.Date()
									.getTime();
							String etStr = "  Elasped Time(ms): "
									+ ((tmpTime - lastTimeMS) / 1000.000)
									+ "                    ";
							System.out
									.println(etStr.substring(0,
											30)
											+ "  Writing record "
											+ (w - 1)
											+ ": "
											+ k
											+ " of "
											+ t
											+ (commit ? " commit"
													: ""));
							lastTimeMS = tmpTime;

						}
					} else {
						String str = "";
						str = str + district.d_id + ",";
						str = str + district.d_w_id + ",";
						str = str + district.d_ytd + ",";
						str = str + district.d_tax + ",";
						str = str + district.d_next_o_id + ",";
						str = str + district.d_name + ",";
						str = str + district.d_street_1 + ",";
						str = str + district.d_street_2 + ",";
						str = str + district.d_city + ",";
						str = str + district.d_state + ",";
						str = str + district.d_zip;
						out.println(str);
					}

				} // end for [d]

			} // end for [w]
			if (outputFiles == false) {
				distPrepStmt.executeBatch();
			}
			transCommit(conn);
			distPrepStmt.close();
			distPrepStmt = null;
			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			System.out.println(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;
			
			now = new java.util.Date();
			System.out.println("End District Load @  " + now);

		} catch (SQLException se) {
			System.out.println(se.getMessage());
			transRollback();
		} catch (Exception e) {
			e.printStackTrace();
			transRollback();
		}

		return (k);

	} // end loadDist()

	private static AtomicInteger ai;

	static int getAutoIncreased() {
		return ai.getAndIncrement();
	}

	static int loadCust(final int whseKount, final int distWhseKount,
			final int custDistKount) throws FileNotFoundException {

		// double cCreditLim = 0;

		final PrintWriter outHist;

		now = new java.util.Date();

		if (outputFiles == true) {
			out = new PrintWriter(new FileOutputStream(fileLocation
					+ "customer.csv"));
			System.out.println("Writing Customer file to: " + fileLocation
					+ "customer.csv");
			outHist = new PrintWriter(new FileOutputStream(fileLocation
					+ "cust-hist.csv"));
			System.out.println("Writing Customer History file to: "
					+ fileLocation + "cust-hist.csv");
		} else {
			out = null;
			outHist = null;
		}

		final int t = ( (whseKount - warehouses_initvalue + 1) * distWhseKount * custDistKount * 2);
		System.out.println("Start Cust-Hist Load for " + t + " Cust-Hists @ "
				+ now + " ...");

		Thread asCall = new Thread() {
			public void run() {
				for (int iw = warehouses_initvalue; iw <= whseKount; iw++) {
					final int w = iw;
					int c = w % parallella;
					final Connection con;
					if (!outputFiles)
						con = conns[c];
					else
						con = null;
					Callable<Integer> runstk = new Callable<Integer>() {
						public Integer call() {
							int k = 0;
							// int i = 1;

							try {
								Customer customer = new Customer();
								History history = new History();
								PreparedStatement custPrepStmt = null;
								PreparedStatement histPrepStmt = null;
								if (!outputFiles) {
									custPrepStmt = con
											.prepareStatement("INSERT INTO benchmarksql.customer "
													+ " (c_id, c_d_id, c_w_id, "
													+ "c_discount, c_credit, c_last, c_first, c_credit_lim, "
													+ "c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, "
													+ "c_street_1, c_street_2, c_city, c_state, c_zip, "
													+ "c_phone, c_since, c_middle, c_data) "
													+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
									histPrepStmt = con
											.prepareStatement("INSERT INTO benchmarksql.history "
													+ " (hist_id, h_c_id, h_c_d_id, h_c_w_id, "
													+ "h_d_id, h_w_id, "
													+ "h_date, h_amount, h_data) "
													+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
								}
								for (int d = 1; d <= distWhseKount; d++) {

									for (int c = 1; c <= custDistKount; c++) {

										sysdate = new java.sql.Timestamp(
												System.currentTimeMillis());

										customer.c_id = c;
										customer.c_d_id = d;
										customer.c_w_id = w;

										// discount is random between [0.0000
										// ...
										// 0.5000]
										customer.c_discount = (float) (jTPCCUtil
												.randomNumber(1, 5000, gen) / 10000.0);

										if (jTPCCUtil.randomNumber(1, 100, gen) <= 90) {
											customer.c_credit = "BC"; // 10% Bad
																		// Credit
										} else {
											customer.c_credit = "GC"; // 90%
																		// Good
																		// Credit
										}
										// customer.c_credit = "GC";

										customer.c_last = jTPCCUtil
												.getLastName(gen);
										customer.c_first = jTPCCUtil
												.randomStr(jTPCCUtil
														.randomNumber(8, 16,
																gen));
										customer.c_credit_lim = 50000;

										customer.c_balance = -10;
										customer.c_ytd_payment = 10;
										customer.c_payment_cnt = 1;
										customer.c_delivery_cnt = 0;

										customer.c_street_1 = jTPCCUtil
												.randomStr(jTPCCUtil
														.randomNumber(10, 20,
																gen));
										customer.c_street_2 = jTPCCUtil
												.randomStr(jTPCCUtil
														.randomNumber(10, 20,
																gen));
										customer.c_city = jTPCCUtil
												.randomStr(jTPCCUtil
														.randomNumber(10, 20,
																gen));
										customer.c_state = jTPCCUtil.randomStr(
												3).toUpperCase();
										customer.c_zip = "123456789";

										customer.c_phone = "(732)744-1700";

										customer.c_since = sysdate.getTime();
										customer.c_middle = "OE";
										customer.c_data = jTPCCUtil
												.randomStr(jTPCCUtil
														.randomNumber(300, 500,
																gen));

										history.hist_id = getAutoIncreased();
										// System.out.println(history.hist_id);
										history.h_c_id = c;
										history.h_c_d_id = d;
										history.h_c_w_id = w;
										history.h_d_id = d;
										history.h_w_id = w;
										history.h_date = sysdate.getTime();
										history.h_amount = 10;
										history.h_data = jTPCCUtil
												.randomStr(jTPCCUtil
														.randomNumber(10, 24,
																gen));

										k = k + 2;
										if (outputFiles == false) {
											custPrepStmt.setLong(1,
													customer.c_id);
											custPrepStmt.setLong(2,
													customer.c_d_id);
											custPrepStmt.setLong(3,
													customer.c_w_id);
											custPrepStmt.setDouble(4,
													customer.c_discount);
											custPrepStmt.setString(5,
													customer.c_credit);
											custPrepStmt.setString(6,
													customer.c_last);
											custPrepStmt.setString(7,
													customer.c_first);
											custPrepStmt.setDouble(8,
													customer.c_credit_lim);
											custPrepStmt.setDouble(9,
													customer.c_balance);
											custPrepStmt.setDouble(10,
													customer.c_ytd_payment);
											custPrepStmt.setDouble(11,
													customer.c_payment_cnt);
											custPrepStmt.setDouble(12,
													customer.c_delivery_cnt);
											custPrepStmt.setString(13,
													customer.c_street_1);
											custPrepStmt.setString(14,
													customer.c_street_2);
											custPrepStmt.setString(15,
													customer.c_city);
											custPrepStmt.setString(16,
													customer.c_state);
											custPrepStmt.setString(17,
													customer.c_zip);
											custPrepStmt.setString(18,
													customer.c_phone);

											Timestamp since = new Timestamp(
													customer.c_since);
											custPrepStmt
													.setTimestamp(19, since);
											custPrepStmt.setString(20,
													customer.c_middle);
											custPrepStmt.setString(21,
													customer.c_data);

											custPrepStmt.addBatch();

											histPrepStmt.setInt(1,
													history.hist_id);
											histPrepStmt.setInt(2,
													history.h_c_id);
											histPrepStmt.setInt(3,
													history.h_c_d_id);
											histPrepStmt.setInt(4,
													history.h_c_w_id);

											histPrepStmt.setInt(5,
													history.h_d_id);
											histPrepStmt.setInt(6,
													history.h_w_id);
											Timestamp hdate = new Timestamp(
													history.h_date);
											histPrepStmt.setTimestamp(7, hdate);
											histPrepStmt.setDouble(8,
													history.h_amount);
											histPrepStmt.setString(9,
													history.h_data);

											histPrepStmt.addBatch();

											if ((k % configCommitCount) == 0) {
												long tmpTime = new java.util.Date()
														.getTime();
												String etStr = "  Elasped Time(ms): "
														+ ((tmpTime - lastTimeMS) / 1000.000)
														+ "                    ";
												System.out.println(etStr
														.substring(0, 30)
														+ "  Writing record   "
														+ (w - 1)
														+ ": "
														+ k
														+ " of " + t);
												lastTimeMS = tmpTime;

												custPrepStmt.executeBatch();
												histPrepStmt.executeBatch();
												custPrepStmt.clearBatch();
												histPrepStmt.clearBatch();
												transCommit(con);
											}
										} else {
											String str = "";
											str = str + customer.c_id + ",";
											str = str + customer.c_d_id + ",";
											str = str + customer.c_w_id + ",";
											str = str + customer.c_discount
													+ ",";
											str = str + customer.c_credit + ",";
											str = str + customer.c_last + ",";
											str = str + customer.c_first + ",";
											str = str + customer.c_credit_lim
													+ ",";
											str = str + customer.c_balance
													+ ",";
											str = str + customer.c_ytd_payment
													+ ",";
											str = str + customer.c_payment_cnt
													+ ",";
											str = str + customer.c_delivery_cnt
													+ ",";
											str = str + customer.c_street_1
													+ ",";
											str = str + customer.c_street_2
													+ ",";
											str = str + customer.c_city + ",";
											str = str + customer.c_state + ",";
											str = str + customer.c_zip + ",";
											str = str + customer.c_phone + ",";
											Timestamp since = new Timestamp(
													customer.c_since);
											str = str + since + ",";
											str = str + customer.c_middle + ",";
											str = str + customer.c_data;
											out.println(str);

											str = "";
											str = str + history.hist_id + ",";
											str = str + history.h_c_id + ",";
											str = str + history.h_c_d_id + ",";
											str = str + history.h_c_w_id + ",";
											str = str + history.h_d_id + ",";
											str = str + history.h_w_id + ",";
											Timestamp hdate = new Timestamp(
													history.h_date);
											str = str + hdate + ",";
											str = str + history.h_amount + ",";
											str = str + history.h_data;
											outHist.println(str);

											if ((k % configCommitCount) == 0) {
												long tmpTime = new java.util.Date()
														.getTime();
												String etStr = "  Elasped Time(ms): "
														+ ((tmpTime - lastTimeMS) / 1000.000)
														+ "                    ";
												System.out.println(etStr
														.substring(0, 30)
														+ "  Writing record "
														+ k + " of " + t);
												lastTimeMS = tmpTime;

											}
										}

									} // end for [c]

								} // end for [d]
								if (outputFiles == true) {
									out.close();
									outHist.close();
								} else {
									custPrepStmt.executeBatch();
									histPrepStmt.executeBatch();
									custPrepStmt.clearBatch();
									histPrepStmt.clearBatch();
									transCommit(con);
									custPrepStmt.close();
									histPrepStmt.close();
									custPrepStmt=null;
									histPrepStmt=null;
								}
							} catch (SQLException se) {
								while (se != null) {
									System.out.println(se.getMessage());
									se.printStackTrace();
									se = se.getNextException();
								}
								transRollback(con);
								if (outputFiles == true) {
									out.close();
									outHist.close();
								}
							} catch (Exception e) {
								e.printStackTrace();
								transRollback(con);
								if (outputFiles == true) {
									out.close();
									outHist.close();
								}
							}

							return k;
						}

					};// end for Callable
					Future<Integer> ft = threadPool.submit(runstk);
					System.out.println("Parallel load Customer "
							+ (new java.util.Date()));
					//add 将指定的元素添加到此队列中（如果立即可行），在成功时返回 true，其他情况则抛出 IllegalStateException。
					//fts.add(ft);
					//offer 将指定的元素插入此队列中，如果没有可用空间，将等待指定的等待时间（如果有必要）。
					try {
						fts.offer(ft, 10, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} // end for [w]
			}
		};
		asCall.start();
		int kk = 0;
		try {
			System.out.println("Waiting Parallel load Customer "
					+ (new java.util.Date()));
			// for (Future<Integer> ft : fts) {
			// kk += ft.get();
			// }
			// fts.clear();
			Future<Integer> ft = fts.take();
			while (ft != null) {
				kk += ft.get();
				ft=null;
				ft = fts.poll();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long tmpTime = new java.util.Date().getTime();
		String etStr = "  Elasped Time(ms): "
				+ ((tmpTime - lastTimeMS) / 1000.000) + "                    ";
		System.out.println(etStr.substring(0, 30) + "  Writing record " + kk
				+ " of " + t);
		lastTimeMS = tmpTime;

		now = new java.util.Date();
		System.out.println("End Cust-Hist Data Load @  " + now);

		return (kk);

	} // end loadCust()

	static int loadOrder(final int whseKount, final int distWhseKount,
			final int custDistKount) throws FileNotFoundException {

		final PrintWriter outO;
		final PrintWriter outLine;
		final PrintWriter outNewOrder;

		if (outputFiles == true) {

			outO = new PrintWriter(new FileOutputStream(fileLocation
					+ "order.csv"));

			System.out.println("Writing Order file to: " + fileLocation
					+ "order.csv");

			outLine = new PrintWriter(new FileOutputStream(fileLocation
					+ "order-line.csv"));

			System.out.println("Writing OrderLine file to: " + fileLocation
					+ "order-line.csv");

			outNewOrder = new PrintWriter(new FileOutputStream(fileLocation
					+ "new-order.csv"));

			System.out.println("Writing NewOrder file to: " + fileLocation
					+ "new-order.csv");

		} else {
			outO = null;
			outLine = null;
			outNewOrder = null;
		}

		now = new java.util.Date();

		int tt = ( (whseKount - warehouses_initvalue + 1) * distWhseKount * custDistKount);
		final int t = (tt * 11) + (tt / 3);
		System.out.println("whse=" + whseKount + ", dist=" + distWhseKount
				+ ", cust=" + custDistKount);
		System.out.println("Start Order-Line-New Load for approx " + t
				+ " rows @ " + now + " ...");
		Thread asCall = new Thread() {
			public void run() {
				for (int iw = warehouses_initvalue; iw <= whseKount; iw++) {
					final int w = iw;
					int c = w % parallella;
					final Connection con;
					if (!outputFiles)
						con = conns[c];
					else
						con = null;
					Callable<Integer> runstk = new Callable<Integer>() {
						public Integer call() {
							int k = 0;
							Oorder oorder = new Oorder();
							NewOrder new_order = new NewOrder();
							OrderLine order_line = new OrderLine();
							jdbcIO myJdbcIO = new jdbcIO();

							try {
								PreparedStatement ordrPrepStmt = null;
								PreparedStatement nworPrepStmt = null;
								PreparedStatement orlnPrepStmt = null;
								if (!outputFiles) {
									ordrPrepStmt = con
											.prepareStatement("INSERT INTO benchmarksql.oorder "
													+ " (o_id, o_w_id,  o_d_id, o_c_id, "
													+ "o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) "
													+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
									nworPrepStmt = con
											.prepareStatement("INSERT INTO benchmarksql.new_order "
													+ " (no_w_id, no_d_id, no_o_id) "
													+ "VALUES (?, ?, ?)");
									orlnPrepStmt = con
											.prepareStatement("INSERT INTO benchmarksql.order_line "
													+ " (ol_w_id, ol_d_id, ol_o_id, "
													+ "ol_number, ol_i_id, ol_delivery_d, "
													+ "ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) "
													+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
								}
								for (int d = 1; d <= distWhseKount; d++) {

									for (int c = 1; c <= custDistKount; c++) {

										oorder.o_id = c;
										oorder.o_w_id = w;
										oorder.o_d_id = d;
										oorder.o_c_id = jTPCCUtil.randomNumber(
												1, custDistKount, gen);
										oorder.o_carrier_id = jTPCCUtil
												.randomNumber(1, 10, gen);
										oorder.o_ol_cnt = jTPCCUtil
												.randomNumber(5, 15, gen);
										oorder.o_all_local = 1;
										oorder.o_entry_d = System
												.currentTimeMillis();

										k++;
										if (outputFiles == false) {
											myJdbcIO.insertOrder(ordrPrepStmt,
													oorder);
										} else {
											String str = "";
											str = str + oorder.o_id + ",";
											str = str + oorder.o_w_id + ",";
											str = str + oorder.o_d_id + ",";
											str = str + oorder.o_c_id + ",";
											str = str + oorder.o_carrier_id
													+ ",";
											str = str + oorder.o_ol_cnt + ",";
											str = str + oorder.o_all_local
													+ ",";
											Timestamp entry_d = new java.sql.Timestamp(
													oorder.o_entry_d);
											str = str + entry_d;
											outO.println(str);
										}

										// 900 rows in the NEW-ORDER table
										// corresponding
										// to the
										// last
										// 900 rows in the ORDER table for that
										// district
										// (i.e.,
										// with
										// NO_O_ID between 2,101 and 3,000)

										if (c > 2100) {

											new_order.no_w_id = w;
											new_order.no_d_id = d;
											new_order.no_o_id = c;

											k++;
											if (outputFiles == false) {
												myJdbcIO.insertNewOrder(
														nworPrepStmt, new_order);
											} else {
												String str = "";
												str = str + new_order.no_w_id
														+ ",";
												str = str + new_order.no_d_id
														+ ",";
												str = str + new_order.no_o_id;
												outNewOrder.println(str);
											}

										} // end new order

										for (int l = 1; l <= oorder.o_ol_cnt; l++) {

											order_line.ol_w_id = w;
											order_line.ol_d_id = d;
											order_line.ol_o_id = c;
											order_line.ol_number = l; // ol_number
											order_line.ol_i_id = jTPCCUtil
													.randomNumber(1, 100000,
															gen);
											order_line.ol_delivery_d = oorder.o_entry_d;

											if (order_line.ol_o_id < 2101) {
												order_line.ol_amount = 0;
											} else {
												// random within [0.01 ..
												// 9,999.99]
												order_line.ol_amount = (float) (jTPCCUtil
														.randomNumber(1,
																999999, gen) / 100.0);
											}

											order_line.ol_supply_w_id = jTPCCUtil
													.randomNumber(1,
															numWarehouses, gen);
											order_line.ol_quantity = 5;
											order_line.ol_dist_info = jTPCCUtil
													.randomStr(24);

											k++;
											if (outputFiles == false) {
												myJdbcIO.insertOrderLine(
														orlnPrepStmt,
														order_line);
											} else {
												String str = "";
												str = str + order_line.ol_w_id
														+ ",";
												str = str + order_line.ol_d_id
														+ ",";
												str = str + order_line.ol_o_id
														+ ",";
												str = str
														+ order_line.ol_number
														+ ",";
												str = str + order_line.ol_i_id
														+ ",";
												Timestamp delivery_d = new Timestamp(
														order_line.ol_delivery_d);
												str = str + delivery_d + ",";
												str = str
														+ order_line.ol_amount
														+ ",";
												str = str
														+ order_line.ol_supply_w_id
														+ ",";
												str = str
														+ order_line.ol_quantity
														+ ",";
												str = str
														+ order_line.ol_dist_info;
												outLine.println(str);
											}

											if ((k % configCommitCount) == 0) {
												long tmpTime = new java.util.Date()
														.getTime();
												String etStr = "  Elasped Time(ms): "
														+ ((tmpTime - lastTimeMS) / 1000.000)
														+ "                    ";
												System.out.println(etStr
														.substring(0, 30)
														+ "  Writing record  "
														+ (w - 1)
														+ ": "
														+ k
														+ " of " + t);
												lastTimeMS = tmpTime;
												if (outputFiles == false) {
													ordrPrepStmt.executeBatch();
													nworPrepStmt.executeBatch();
													orlnPrepStmt.executeBatch();
													ordrPrepStmt.clearBatch();
													nworPrepStmt.clearBatch();
													orlnPrepStmt.clearBatch();
													transCommit(con);
												}
											}

										} // end for [l]

									} // end for [c]

								} // end for [d]
								if (outputFiles == true) {
									outO.close();
									outLine.close();
									outNewOrder.close();
								} else {
									ordrPrepStmt.executeBatch();
									nworPrepStmt.executeBatch();
									orlnPrepStmt.executeBatch();
									ordrPrepStmt.clearBatch();
									nworPrepStmt.clearBatch();
									orlnPrepStmt.clearBatch();
									transCommit(con);
									ordrPrepStmt.close();
									nworPrepStmt.close();
									orlnPrepStmt.close();
									ordrPrepStmt=null;
									nworPrepStmt=null;
									orlnPrepStmt=null;
								}

							} catch (SQLException se) {
								while (se != null) {
									System.out.println(se.getMessage());
									se = se.getNextException();
								}
								transRollback(con);
								if (outputFiles == true) {
									outO.close();
									outLine.close();
									outNewOrder.close();
								}
							} catch (Exception e) {
								e.printStackTrace();
								transRollback(con);
								if (outputFiles == true) {
									outO.close();
									outLine.close();
									outNewOrder.close();
								}
							}

							return k;

						}
					}; // end for Callable
					Future<Integer> ft = threadPool.submit(runstk);
					System.out.println("Parallel load Order "
							+ (new java.util.Date()));
					//add 将指定的元素添加到此队列中（如果立即可行），在成功时返回 true，其他情况则抛出 IllegalStateException。
					//fts.add(ft);
					//offer 将指定的元素插入此队列中，如果没有可用空间，将等待指定的等待时间（如果有必要）。
					try {
						fts.offer(ft, 10, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} // end for [w]
			}
		};
		asCall.start();

		int kk = 0;
		try {
			System.out.println("Waiting Parallel load Order "
					+ (new java.util.Date()));
			// for (Future<Integer> ft : fts) {
			// kk += ft.get();
			// }
			// fts.clear();
			//take 检索并移除此队列的头部，如果此队列不存在任何元素，则一直等待。
			Future<Integer> ft = fts.take();
			while (ft != null) {
				kk += ft.get();
				ft=null;
				//poll 检索并移除此队列的头部，如果此队列中没有任何元素，则等待指定等待的时间（如果有必要）。
				ft = fts.poll();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		long tmpTime = new java.util.Date().getTime();
		String etStr = "  Elasped Time(ms): "
				+ ((tmpTime - lastTimeMS) / 1000.000) + "                    ";
		System.out.println(etStr.substring(0, 30) + "  Writing final records "
				+ kk + " of " + t);
		lastTimeMS = tmpTime;

		now = new java.util.Date();
		System.out.println("End Orders Load @  " + now);

		return (kk);

	} // end loadOrder()

} // end LoadData Class
