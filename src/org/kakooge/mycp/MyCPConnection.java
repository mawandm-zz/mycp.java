package org.kakooge.mycp;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.logging.Logger;


/**
 * Encapsulates a given connection
 * @author Michael Sekamanya
 *
 */
public class MyCPConnection implements Connection{
	final private Connection underlyingConnection;
	final private PoolManager poolManager;
	private int connectionId;
	
	final private boolean debug = System.getProperty("debug")!=null;
	
	final static private Logger logger = Logger.getLogger(MyCPConnection.class.getName());
	
	/**
	 * Constructs a connection wrapper object. It is intended to inject an already acquired connection to avoid
	 * blocking during construction if say {@code poolDeque.getFirst()} was called
	 * @param poolDeque the underlying {@link BlockingDeque}. This is only supplied so that {@code underlyingConnection} can be
	 * returned back to the pool at {@link #close}
	 * @param underlyingConnection the underlying {@link Connection} to be wrapped
	 */
	public MyCPConnection(final PoolManager poolManager, final Connection underlyingConnection){
		this.poolManager = poolManager;
		this.underlyingConnection = underlyingConnection;
	}
	
	private void validateUnderlyingConnection() throws SQLException{
		if(underlyingConnection==null)
			throw new SQLException("Invalid underlying connection");
	}
	
	public int getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(int connectionId) {
		this.connectionId = connectionId;
	}

	public Connection getUnderlyingConnection() {
		return underlyingConnection;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.isWrapperFor(arg0);
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.unwrap(arg0);
	}

	@Override
	public void clearWarnings() throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.clearWarnings();
	}

	@Override
	public void close() throws SQLException {
		try {
			if(!poolManager.returnConnection(this)){
				if(debug)
					System.out.println("Unable to return connection .... destroying underlying connection");
				try{
					if(underlyingConnection!=null)
						underlyingConnection.close();
				}catch(SQLException ignore){}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SQLException(e);
		}
	}

	@Override
	public void commit() throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.commit();
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createArrayOf(arg0, arg1);
	}

	@Override
	public Blob createBlob() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createBlob();
	}

	@Override
	public Clob createClob() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createClob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createSQLXML();
	}

	@Override
	public Statement createStatement() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createStatement();
	}

	@Override
	public Statement createStatement(int arg0, int arg1) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createStatement(arg0, arg1);
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2)
			throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createStatement(arg0, arg0, arg2);
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.createStruct(arg0, arg1);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getAutoCommit();
	}

	@Override
	public String getCatalog() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getCatalog();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getClientInfo();
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getClientInfo(arg0);
	}

	@Override
	public int getHoldability() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getHoldability();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getMetaData();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getTransactionIsolation();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getTypeMap();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.getWarnings();
	}

	@Override
	public boolean isClosed() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.isClosed();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.isReadOnly();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		if(underlyingConnection == null){
			logger.info("invalid underlyingConnection = null");
			return false;
		}
		return underlyingConnection.isValid(timeout);
	}

	@Override
	public String nativeSQL(String arg0) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.nativeSQL(arg0);
	}

	@Override
	public CallableStatement prepareCall(String arg0) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareCall(arg0);
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
			throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareCall(arg0, arg1, arg2);
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareCall(arg0, arg1, arg2, arg3);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareStatement(sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareStatement(sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareStatement(sql, columnNames);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.releaseSavepoint(savepoint);
	}

	@Override
	public void rollback() throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.rollback();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.rollback(savepoint);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.setAutoCommit(autoCommit);
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.setCatalog(catalog);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		if(underlyingConnection == null)
			throw new SQLClientInfoException("Invalid underlying connection", new HashMap<String, ClientInfoStatus>());
		underlyingConnection.setClientInfo(properties);
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		if(underlyingConnection == null)
			throw new SQLClientInfoException("Invalid underlying connection", new HashMap<String, ClientInfoStatus>());
		underlyingConnection.setClientInfo(name, value);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.setHoldability(holdability);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.setReadOnly(readOnly);
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		validateUnderlyingConnection();
		return underlyingConnection.setSavepoint(name);
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.setTransactionIsolation(level);
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		validateUnderlyingConnection();
		underlyingConnection.setTypeMap(map);
	}
	
}

class Configuration{

	final public static String MYCP_MAX_CONNECTIONS = "mycp.max.connections";
	final public static String MYCP_MIN_CONNECTIONS = "mycp.min.connections";
	final public static String MYCP_KEEP_ALIVE_SQL = "mycp.keep.alive.sql";
	final public static String MYCP_DRIVER = "mycp.driver";
	final public static String MYCP_DRIVER_URL = "mycp.driver.url";
	final public static String MYCP_MAX_WAIT = "mycp.max.wait";
	
	final private Properties properties;
	
	public Configuration(final Properties customProperties) throws MyCPException{
		properties = initDefaultProperties();
		initCustomProperties(customProperties);
	}
		
	private void initCustomProperties(final Properties customProperties) throws MyCPException{
		try{
			int minConnections = 0, maxConnections = 0;
			if(!StringUtil.Empty(customProperties.getProperty(MYCP_MAX_CONNECTIONS))){
				maxConnections = new Integer(customProperties.getProperty(MYCP_MAX_CONNECTIONS));
				this.properties.put(MYCP_MAX_CONNECTIONS, new Integer(maxConnections));
			}
			
			if(!StringUtil.Empty(customProperties.getProperty(MYCP_MIN_CONNECTIONS))){
				minConnections = new Integer(customProperties.getProperty(MYCP_MIN_CONNECTIONS));
				this.properties.put(MYCP_MIN_CONNECTIONS, new Integer(minConnections));
			}
			
			if(minConnections<0 || maxConnections<0 || minConnections > maxConnections)
				throw new MyCPException("MyCP Config failure, minConnections is greater than maxConnections");
			
			if(!StringUtil.Empty(customProperties.getProperty(MYCP_MAX_WAIT)))
				properties.put(MYCP_MAX_WAIT, Long.parseLong(customProperties.getProperty(MYCP_MAX_WAIT)));
			
		}catch(NumberFormatException e){
			throw new MyCPException(String.format("MyCP Config failure, check that '%s', '%s', " +
					"'%s' are valid numbers", MYCP_MAX_CONNECTIONS, MYCP_MIN_CONNECTIONS, MYCP_MAX_WAIT), e);
		}
		
		final String keepAliveSQL = customProperties.getProperty(MYCP_KEEP_ALIVE_SQL);
		if(!StringUtil.Empty(keepAliveSQL))
			properties.put(MYCP_KEEP_ALIVE_SQL, keepAliveSQL);
		
		final String mycpDriver = customProperties.getProperty(MYCP_DRIVER);
		if(StringUtil.Empty(mycpDriver))
			throw new MyCPException(String.format("MyCP Config failure, specify database driver with '%s'", MYCP_MAX_CONNECTIONS, MYCP_DRIVER));
		properties.put(MYCP_DRIVER, mycpDriver);
		
		final String url = customProperties.getProperty(MYCP_DRIVER_URL);
		if(StringUtil.Empty(url))
			throw new MyCPException(String.format("MyCP Config failure, specify database driver url with '%s'", MYCP_DRIVER_URL));
		properties.put(MYCP_DRIVER_URL, url);
	}
	
	private Properties initDefaultProperties(){
		final Properties defaultProperties = new Properties();
		defaultProperties.put(MYCP_DRIVER_URL, "");
		defaultProperties.put(MYCP_KEEP_ALIVE_SQL, "");
		defaultProperties.put(MYCP_MAX_CONNECTIONS, new Integer(Integer.MAX_VALUE));
		defaultProperties.put(MYCP_MIN_CONNECTIONS, new Integer(10));
		defaultProperties.put(MYCP_KEEP_ALIVE_SQL, "");
		defaultProperties.put(MYCP_DRIVER, "");
		defaultProperties.put(MYCP_DRIVER_URL, "");
		defaultProperties.put(MYCP_MAX_WAIT, new Long(Long.MAX_VALUE));
		
		return defaultProperties;
	}
	
	public long getMaxWaitForConnection() {
		return (Long)properties.get(MYCP_MAX_WAIT);
	}

	public String getUrl() {
		return properties.get(MYCP_DRIVER_URL).toString();
	}

	public String getDriver() {
		return properties.get(MYCP_DRIVER).toString();
	}

	public int getMaxConnections() {
		return (Integer)properties.get(MYCP_MAX_CONNECTIONS);
	}

	/**
	 * 
	 * @return
	 */
	public int getMinConnections() {
		return (Integer)properties.get(MYCP_MIN_CONNECTIONS);
	}

	/**
	 * Keep alive SQL specific to the connecting database such as {@code select 1} for a MySQL database
	 * @return the SQL statement or and empty string if none was specified
	 */
	public String getKeepAliveSQL() {
		return properties.get(MYCP_KEEP_ALIVE_SQL).toString();
	}
		
}

