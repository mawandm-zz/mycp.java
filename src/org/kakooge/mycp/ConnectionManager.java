package org.kakooge.mycp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;


/**
 * Manage connections. Uses ordinary statics (moving averages) to determine some values e.g.
 * <ol>
 * <li>For all connections not addable immediately to the pool, run the keep alive sql and then wait for the average wait time before the next connection is taken</li>
 * <li>For each management cycle, run and wait for the average wait time before the process completes (or receives a pause signal)</li>
 * </ol>
 * <p><b>Interrupt policy</b></p>
 * This thread can be interrupted for various reasons e.g. Termination, Pausing the connection management cycle
 * @author Michael Sekamanya
 *
 */
public class ConnectionManager extends Thread{
	private final Configuration configuration; 
	private final Properties driverProperties;
	private final PoolManager poolManager;
	
	/**
	 * This is the number of connections that are currently managed by the pool. We keep the number here
	 * because as the connections are taken out of the pool the pool size reduces
	 */
	private int connectionCount = 0;
	
	/**
	 * Used to receive signals from the environment such as
	 * <ol>
	 * <li>SIGTERM - Terminate the thread</li>
	 * <li>SIGSTOP - Pause during clean up (basically once an average idle time has elapsed, the connection
	 * manager will be signalled to start a clean up process. During the clean up however, the connection 
	 * manager can be prompted to stop processing if any activity is detected within the pool</li>
	 * <li>SIGPOOL - Check the thread - we might need more connection or to reduce connections</li>
	 * <li></li> 
	 * </ol>
	 * 
	 */
	private volatile int signal = 0;
	
	/**
	 * Terminate this thread at the earliest possible time
	 */
	final private static int SIGTERM = 1<<0;
	
	/**
	 * Stop this thread almost immediately
	 */
	final private static int SIGSTOP = 1<<1;
	
	/**
	 * If thread is currently stopped, continue processing
	 */
	final private static int SIGCONT = 1<<2;
	
	/**
	 * Wake up and check the pool, we might need some more connections
	 */
	final private static int SIGPOOL = 1<<3;
	
	/**
	 * Enable debugging
	 */
	final private boolean debug = System.getProperty("debug")!=null;
	
	final static Logger logger = Logger.getLogger(ConnectionManager.class.getName());
	
	public ConnectionManager(final Configuration configuration, 
			final Properties driverProperties,
			final PoolManager poolManager){
		this.configuration = configuration;
		this.driverProperties = driverProperties;
		this.poolManager = poolManager;
	}
	
	/************************************************************************************
	 *    		POOL MANAGEMENT ROUTINES 
	 ************************************************************************************/
	
	/**
	 * Add a connection to the pool. This process keeps a statistic of the average wait time
	 * for the application before a connection is successfully inserted. It then uses it as a kinda moving average
	 * @param connection the underlying {@link Connection} to abstract
	 * @throws InterruptedException if interrupted during polling to see if an insertion is possible
	 */
	private boolean addToPool(Connection connection) throws InterruptedException{
		final MyCPConnection mycpConnection = new MyCPConnection(poolManager, connection);
		boolean result = false;
	
		result = poolManager.returnConnection(mycpConnection);
		/**
		 * If we fail to return the connection because for example the pool is full. Then the connection manager
		 * hasn't been paused, attempt to keep the connection alive by executing the keep alive SQL in this case.
		 * Kill the connection if the process fails
		 */
		while(!result && !isPaused()){
			//- Attempt to keep the connection alive by running the keep alive SQL
			//- This is done for a maximum of the average wait time before a connection is taken off the queue
			result = poolManager.returnConnection(mycpConnection);
		}

		return result;
	}
	
	/**
	 * Increments the connection pool by 1/10 of the maximum number of connections
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private void makeConnection() throws SQLException, InterruptedException{
		
		final int availableConnections = poolManager.getPoolSize();
		final int maxConnections = configuration.getMaxConnections();
		final int minConnections = configuration.getMinConnections();
		final int incrementTo = Math.round(0.15f * connectionCount);
		String url = configuration.getUrl();
		if(StringUtil.Empty(url))
			throw new SQLException("Missing url, please specify one");
		
		final int incrementValue = availableConnections < minConnections ? (minConnections - availableConnections) : 
			Math.round(0.1f * maxConnections); 
		
		/**
		 * If the currently available connections is less than 1/4 of the original number of connections
		 * then increment by incrementValue
		 */
		if(availableConnections <= incrementTo){
			
			if(debug)
				logger.info("decrementing to: " + incrementTo);
			
			for(int count = 0; count < incrementValue && connectionCount <= maxConnections; ++count){
				Connection connection = null;
				
				if(isPaused())
					return;
				
				try{
					connection = DriverManager.getConnection(url, driverProperties);
				}catch(RuntimeException re){
					connection = DriverManager.getConnection(url);
				}
				if(connection == null)
					throw new SQLException(String.format("Could not establish jdbc connection to url '%s'", url));
				
				boolean addResult = false;
				try{
					addResult = addToPool(connection);
				}finally{
					if(addResult)
						++connectionCount;
					else{
						if(connection!=null){
							try{
								connection.close();
							}catch(SQLException ignore){}
						}
					}
				}
			}
		}
	}
	
	/**
	 * Decrements the pool by 1/10 of the maximum number of connections
	 * @throws InterruptedException
	 */
	private void killConnection() throws InterruptedException{
		
		final int availableConnections = poolManager.getPoolSize();
		final int maxConnections = configuration.getMaxConnections();
		final int minConnections = configuration.getMinConnections();
		final int decrementValue = Math.round(0.1f * maxConnections); 
		final int decrementTo = Math.round(0.75f * connectionCount);
		
		/**
		 * Remove older connections one by one until 1/10 of the maximum connection permitted
		 * Abort if the process is paused
		 */
		if(availableConnections >= decrementTo && availableConnections > minConnections){
			
			if(debug)
				logger.info(String.format("Connection count: %d, decrementing to: %d", connectionCount, decrementValue));
			
			for(int idx = 0; idx<decrementValue && connectionCount > minConnections; ++idx){
				if(isPaused())
					return;
				MyCPConnection mycpConnection = poolManager.takeConnection();
				Connection connection = mycpConnection.getUnderlyingConnection();
				if(connection!=null){
					try {
						connection.close();
					} catch (SQLException e) {}
				}
				--connectionCount;
			}
		}
	}
	
	/*********************************************************************************
	 * Thread management routines
	 ********************************************************************************/
	/**
	 * Terminates the manager thread at the earliest possible time
	 */
	public synchronized void terminate(){
		if(debug)
			logger.info("Terminating ConnectionManager");
		signal |= ConnectionManager.SIGTERM;
		interrupt();
	}
	
	/**
	 * Check if the current thread is/will-be terminating soon
	 * @return
	 */
	public boolean isTerminated(){
		return (signal & ConnectionManager.SIGTERM) == ConnectionManager.SIGTERM;
	}
	
	/**
	 * Pauses this thread so that the pool can be used. Particularly useful during connection cleanup
	 */
	public synchronized void pause(){
		signal |= ConnectionManager.SIGSTOP;
		interrupt();
	}
	
	public boolean isPaused(){
		return (signal & ConnectionManager.SIGSTOP) == ConnectionManager.SIGSTOP;
	}

	public synchronized void checkPool(){
		signal |= ConnectionManager.SIGPOOL;
		interrupt();		
	}
	
	public boolean isCheckPool(){
		return (signal & ConnectionManager.SIGPOOL) == ConnectionManager.SIGPOOL;
	}	
	
	/**
	 * Calculates the wait time depending on the current thread status as indicated by {@link #status}
	 * @return
	 */
	private long sleepLength(){
		if(isPaused())
			return 60 * 1000;
		return 60 * 1000;
	}
	
	@Override
	public void run() {
		while(!isTerminated()){
			/*
			 * Simple algorithm
			 * 1. If the number of connections in poolQueue is increasing, then destroy some connections
			 * 2. If the number of connections in the poolQueue is decreasing, then create some connections by a magnitude of 10 to the maximum connections
			 */
			try {
				makeConnection();
				killConnection();
				if(debug)
					logger.info("Connection count: " + connectionCount);
				Thread.sleep(sleepLength());
			} catch (InterruptedException e) {
				if(debug)
					logger.info("interrupted");
				// If the interrupt signal is due to a pause, sleep for an average time
				if(isPaused()){
					if(debug)
						logger.info("interrupted by pause");
					try {
						Thread.sleep(sleepLength());
					} catch (InterruptedException ignore) {}
				}
				if(isCheckPool()){
					if(debug)
						logger.info("interrupted to check pool");				
				}
				
				Thread.currentThread().interrupt();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		if(debug)
			logger.info("ConnectionManager terminating...");
	}
}

