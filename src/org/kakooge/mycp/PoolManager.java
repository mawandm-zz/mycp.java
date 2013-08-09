package org.kakooge.mycp;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class PoolManager{
	private final Configuration configuration;
	/**
	 * The main underlying pool structure
	 */
	private final BlockingQueue<MyCPConnection> poolQueue;
	private final Properties driverProperties;
	private ConnectionManager connectionManagerThread;
	private final boolean debug = System.getProperty("debug")!=null;
	
	final static private Logger logger = Logger.getLogger(PoolManager.class.getName());
	
	public PoolManager(final Configuration configuration, final Properties driverProperties){
		
		this.configuration = configuration;
		this.driverProperties = driverProperties;
		poolQueue = new LinkedBlockingQueue<MyCPConnection>(configuration.getMaxConnections());
	}
	
	public void init(){
		connectionManagerThread = new ConnectionManager(configuration, driverProperties, this);
		/*
		for(int i=0; i<configuration.getMinConnections(); ++i){
			String url = configuration.getUrl();
			Connection connection;
			try {
				connection = DriverManager.getConnection(url);
				MyCPConnection mycpConnection = new MyCPConnection(this, connection);
				mycpConnection.getConnectionId();
				poolQueue.put(mycpConnection);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		*/
		
		if(debug)
			logger.info("initializing poolmanager");
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				PoolManager.this.destroy();
			}
		});
		
		connectionManagerThread.start();
	}
	
	public void destroy(){
		if(debug)
			logger.info("de-initializing poolManager");
		connectionManagerThread.terminate();
		try {
			connectionManagerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		/**
		 * Keep trying to destroy the connections. Drain until pool is completely drained
		 */
		while(poolQueue.size()>0){
			final Collection<MyCPConnection> collection = new ArrayList<MyCPConnection>(poolQueue.size());
			logger.info(String.format("Killing %d connections", collection.size()));
			
			for(final MyCPConnection mycpConnection : collection){
				if(mycpConnection==null)
					continue;
				try{
					final Connection connection = mycpConnection.getUnderlyingConnection();
					if(connection!=null)
						connection.close();
				}catch(Exception e){
				}
			}
		}
	}
	
	public MyCPConnection takeConnection() throws InterruptedException{
		/*
		if(debug)
			System.out.println(String.format("Thread %d taking connection", Thread.currentThread().getId()));

		if(debug)
			System.out.println(String.format("Thread %d Before pool size = %d", Thread.currentThread().getId(), poolQueue.size()));
		*/
		MyCPConnection connection = poolQueue.poll(configuration.getMaxWaitForConnection(), TimeUnit.SECONDS);
		
		/*
		 * This causes the pool, if sleeping, to wake up and check that the pool number are OK
		 */
		/*
		if(poolQueue.size()==0)
			connectionManagerThread.checkPool();
		*/
		
		if(connection == null)
			connection = new MyCPConnection(this, null);
		
		/*
		if(debug)
			System.out.println(String.format("Thread %d After pool size = %d", Thread.currentThread().getId(), poolQueue.size()));
		*/
		return connection;
	}

	/**
	 * Returns the supplied {@link MyCPConnection} to the pool. All this does is to execute {@link BlockingQueue#offer(Object)}
	 * and returns the result of that operation
	 * @param mycpConnection
	 * @return
	 * @throws InterruptedException
	 */
	public boolean returnConnection(final MyCPConnection mycpConnection) throws InterruptedException{
		boolean result = poolQueue.offer(mycpConnection);
		/*
		if(debug)
			logger.info("Returning the connection: " + poolQueue.size());
		*/
		return result;
	}
	
	public int getPoolSize(){
		return poolQueue.size();
	}
	
	public int drainTo(Collection<MyCPConnection> drainedConnections, int connectionCount){
		return poolQueue.drainTo(drainedConnections, connectionCount);
	}
}