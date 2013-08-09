package org.kakooge.mycp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Main {
	
	final static private Logger logger = Logger.getLogger(Main.class.getName());
	
	private static void testMySQL(final Properties properties) throws ClassNotFoundException, SQLException{
		String url = "jdbc:mysql://localhost:3306/nafasi?user=nfsuser&password=NaPHa5!";
		Class.forName("com.mysql.jdbc.Driver");
		Connection connection = DriverManager.getConnection(url);
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("select 1");
		System.out.println(rs.next() ? rs.getInt(1) : 0);
	}

	private static void testMyCP(final String url, final Properties properties) throws SQLException{
		
		Connection connection = DriverManager.getConnection(url);
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("select 1");
		System.out.println(rs.next() ? rs.getInt(1) : 0);
	}
	
	private static Properties getProperties(final String file) throws IOException{
		final Properties properties = new Properties();
		InputStream in = null;
		try{
			in = new FileInputStream(file);
			properties.load(in);
		}finally{
			try{
				in.close();
			}catch(Exception e){}
		}

		return properties;
	}
	
	private static void testMyCPManager(final String url, final Properties properties) throws IOException, MyCPException{
		final int n = 15;
		final Properties mycpProperties = getProperties("/host/Users/mawandm/Documents/Projects/kiboel/mycp.java/src/kjdbc.properties");
		final Configuration configuration = new Configuration(mycpProperties);
		final PoolManager poolManager = new PoolManager(configuration, properties);

		try{
			poolManager.init();
			
			ConnectionManager connectionManager = new ConnectionManager(configuration, mycpProperties, poolManager);
			connectionManager.run();
		}finally{
			poolManager.destroy();
		}
		//- Create N connections
		//- 
	}
	
	private static void testMyCPHeavyLoad(final String url, final Properties properties) throws InterruptedException{
		TestVariableLoad.testMyCPHeavyLoad(url, properties);
	}
	
	private static void testMyCPMultiple(final String url, final Properties properties){
		final int noOfThreads = 80;
		final CountDownLatch startLatch = new CountDownLatch(1);
		final CountDownLatch endLatch = new CountDownLatch(noOfThreads);
		
		class TestConnectionThread extends Thread{
			
			private int missCount;
			private final long id;
			
			public TestConnectionThread(long id){
				this.id = id;
			}
			
			@Override
			public long getId(){
				return id;
			}

			private void process(){
				Connection connection = null;
				try {
					//System.out.println(String.format("Thread %d attempting to obtain connection", id));
					connection = DriverManager.getConnection(url, properties);
					if(connection==null){
						System.out.println(String.format("Thread %d could not get connection", id));
						return;
					}else if(!connection.isValid(0)){
						++missCount;
						System.out.println(String.format("Thread %d could not get a valid connection - missCount = %d", id, missCount));
						return;						
					}
					missCount=0;
					
					// do some serious random work here
					Statement statement = null;
					ResultSet result = null;
					try{
						statement = connection.createStatement();
						result = statement.executeQuery("select count(*) from log");
						if(result!=null && result.next())
							System.out.println(String.format("Thread %d @ %s - result %d", id, new Date(), result.getInt(1)));
						Thread.sleep(30000 + new Random(System.currentTimeMillis()).nextInt(5 * 6 * 1000));
					} catch (InterruptedException e) {
					}finally{
						try{
							result.close();
						}catch(Exception e){}
						
						try{
							statement.close();
						}catch(Exception e){}
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}finally{
					try{
						connection.close();
					}catch(Exception e){}
				}
			}
			
			@Override
			public void run() {
				final int count = 1000000;
				int iterations = 0;
				while(++iterations < count){
					process();
					try {
						final int sleepLength = new Random(System.currentTimeMillis()).nextInt(2 * 1000)
								+ new Random(System.currentTimeMillis()).nextInt(2 * 1000);
						System.out.println(String.format("Thread %d sleeping for %d", id, sleepLength));
						Thread.sleep(sleepLength);
					} catch (InterruptedException e) {
						interrupt();
					}
				}
			}
		};
		
		final Thread[] tArray = new Thread[noOfThreads]; 
		
		for(int i = 0; i < noOfThreads; i++){
			final TestConnectionThread connectionThread = new TestConnectionThread(i);
			tArray[i] = new Thread(){
				@Override
				public void run() {
					try {
						//startLatch.await();
						connectionThread.start();
					//} catch (InterruptedException e) {
					}finally{
						//endLatch.countDown();
					}
				}
			};
			tArray[i].start();
		}
		
		
		for(int idx = 0; idx<tArray.length; ++idx){
			try {
				tArray[idx].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		final Properties properties = new Properties();
		properties.put("user", "nfsuser");
		properties.put("password", "NaPHa5!");
		properties.put("autoReconnectForPools", "true");
		
		
		logger.info("Right here");
		
		//testMySQL(properties);
	
		String url = "jdbc:kiboel:/host/Users/mawandm/Documents/Projects/kiboel/mycp.java/src/kjdbc.properties";
		Class.forName("org.kakooge.mycp.JdbcDriver");
		
		//testMyCPManager(url, properties);
	
		//testMyCP(url, properties);
		//testMyCPMultiple(url, properties);
		
		testMyCPHeavyLoad(url, properties);
	}
}

class TestTask implements Runnable{
	
	//private int missCount;
	private final long id;
	private String url;
	private Properties properties;
	
	public TestTask(long id){
		this.id = id;
	}

	public void setProperties(final Properties properties){
		this.properties = properties;
	}
	
	public void setUrl(final String url){
		this.url = url;
	}

	private void process(){
		Connection connection = null;
		try {
			//System.out.println(String.format("Thread %d attempting to obtain connection", id));
			connection = DriverManager.getConnection(url, properties);
			/*
			if(connection==null){
				System.out.println(String.format("Thread %d could not get connection", id));
				return;
			}else if(!connection.isValid(0)){
				++missCount;
				System.out.println(String.format("Thread %d could not get a valid connection - missCount = %d", id, missCount));
				return;						
			}
			missCount=0;
			*/
			
			// do some serious random work here
			Statement statement = null;
			ResultSet result = null;
			try{
				statement = connection.createStatement();
				result = statement.executeQuery("select count(*) from log");
				if(result!=null && result.next())
					System.out.println(String.format("Thread %d @ %s - result %d", id, new Date(), result.getInt(1)));
				final long sleepFor = pause(3000 + new Random(System.currentTimeMillis()).nextInt(30 * 1000));
				Thread.sleep(sleepFor);
			} catch (InterruptedException ignore) {
				
			}finally{
				try{
					result.close();
				}catch(Exception e){}
				
				try{
					statement.close();
				}catch(Exception e){}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try{
				connection.close();
			}catch(Exception e){}
		}
	}
	
	private long pause(int y){
		return new Random(System.currentTimeMillis()).nextInt(y * 
				new Random(System.currentTimeMillis()).nextInt(5))
				+ new Random(System.currentTimeMillis()).nextInt(y);
	}
	
	@Override
	public void run() {
		process();
		try {
			final long sleepLength = pause(1000);
			//System.out.println(String.format("Thread %d sleeping for %d", id, sleepLength));
			Thread.sleep(sleepLength);
		} catch (InterruptedException ignore) {}
	}
}


class Cordinate<T>{
	T x;
	T y;
	
	public Cordinate(T x, T y){
		this.x = x;
		this.y = y;
	}
}

class TaskGenerator implements Runnable{
	
	/**
	 * The task queue
	 */
	final ExecutorService executor;
	
	final long min;
	final long max;
	final String url;
	final Properties properties;
	
	final static Logger logger = Logger.getLogger(TaskGenerator.class.getName());
	
	public TaskGenerator(final ExecutorService executor, final long min, final long max, final String url, final Properties properties){
		this.executor = executor;
		this.min = min;
		this.max = max;
		this.url = url;
		this.properties = properties;
	}
	
	private long taskCount = 1;
	
	@Override
	public void run() {
		final long noTasks = getDesiredTaskNo();
		logger.info(String.format("Adding %d", noTasks));
		for(int idx=0; idx<noTasks; ++idx){
			final TestTask testTask = new TestTask(taskCount++);
			testTask.setProperties(properties);
			testTask.setUrl(url);
			executor.execute(testTask);
			/*try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}*/
		}
		//taskCount += noTasks;
	}
	
	/**
	 * used to keep the previous cordinate as it will be used to generate the next cordinate
	 */
	private Cordinate<Double> previous = new Cordinate<Double>(new Double(0), new Double(0));
	
	/**
	 * Get the desired number of tasks to generate; using {@code f(x) = min*(M-(M-1)*cos(*(1+R())))}
	 * where {@code M} is the max factor that controls the maximum of the function and {@code R} is random
	 * generator between 0 and 1. {@code min} is the minimum number of threads in the task queue
	 * @param previous
	 * @return
	 */
	private long getDesiredTaskNo(){
		final double newX = previous.x + Math.random() * 0.1;
		previous.x = newX;
		final double angle = Math.PI * newX;
		/*
		final double min = 20;
		*/
		final double maxfactor = (double)max/(double)min/2;
		
		final double newY = min * (maxfactor - (maxfactor - 1) * Math.cos(angle * (1 + Math.random())));
		previous.y = newY;
		return (long)newY;
	}
}

class TestVariableLoad{
	
	private final static Logger logger = Logger.getLogger(TestVariableLoad.class.getName());
	
	private static boolean addNewTask(final ThreadPoolExecutor executor){
		final double completedTasks = (double)executor.getCompletedTaskCount();
		final double totalTasks = (double)executor.getTaskCount();
		final int activeTasks = executor.getActiveCount();
		final double load = completedTasks/totalTasks;
		logger.info(String.format("Status: active = %d, completed = %s, total = %s, load = %s", activeTasks, completedTasks, totalTasks, load));
		return (load >= 0.95);
	}
	
	public static void testMyCPHeavyLoad(final String url, final Properties properties) throws InterruptedException{
		final int minPoolSize = 1000;
		final int maxPoolSize = 20000;
		final ThreadPoolExecutor executorService = new ThreadPoolExecutor(minPoolSize, maxPoolSize, Long.MAX_VALUE, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		final TaskGenerator taskGenerator = new TaskGenerator(executorService, minPoolSize, maxPoolSize, url, properties);
		
		final int maxIteration = 10;
		int iterationCount = 0;
		while(iterationCount++ < maxIteration){
			taskGenerator.run();
			while(!addNewTask(executorService))
				Thread.sleep(5000);
		}
		
		while(executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS));
	}
}
