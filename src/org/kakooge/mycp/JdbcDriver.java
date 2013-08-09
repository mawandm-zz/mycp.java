package org.kakooge.mycp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class JdbcDriver implements Driver{

    
	/**
	 * Properties for controlling this connection pool driver
	 * <pre>
	 * mycp.maximum.connections = 5
	 * mycp.keep.alive.sql=select 1
	 * </pre>
	 */

	private PoolManager poolManager = null;
	
	   
    protected boolean debug = false;
    
    static{
    	try{
    		DriverManager.registerDriver(new JdbcDriver());
    	}catch(SQLException e){
    		throw new Error(e);
    	}
    }
	
	private String getConfigPath(String url){
        
        String jdbcDeclaration = "jdbc:kiboel:";
        if(url!=null && url.startsWith(jdbcDeclaration)){
        	return url.substring(jdbcDeclaration.length());
        }
        return null;
    }

    private Properties initProperties(String propertiesFileName) throws IOException{
		InputStream in = null;
		final Properties prop = new Properties();
		
		try{
			try{
				in = new FileInputStream(propertiesFileName);
			}catch(FileNotFoundException e){
				if(!propertiesFileName.startsWith("/"))
					propertiesFileName = "/" + propertiesFileName;
				in = JdbcDriver.class.getResourceAsStream(propertiesFileName);
			}
		    prop.load(in);
		    return prop;
		}finally{
		    if(in!=null) in.close();
		}
    }
    
    /**
     * @throws MyCPException 
     * 
     */
    private synchronized void initConnectionPool(final Properties mycpProperties, final Properties driverProperties) throws MyCPException{

    	if(poolManager!=null)
    		return;
    	
    	Configuration configuration = new Configuration(mycpProperties);
    	poolManager = new PoolManager(configuration, driverProperties);
    	
    	String driver = configuration.getDriver();
		try{
		    Class.forName(driver); //loads the jdbc driver
		}catch(ClassNotFoundException e){
			if(debug)
				e.printStackTrace();
		    throw new Error(e);
		}
		
		poolManager.init();
    }

	@Override
    public boolean acceptsURL(String url) {
        String configPath = getConfigPath(url);
        if(configPath == null)
            return false;

        File fileConfig = new File(configPath);
        if(!fileConfig.exists())
            return false;

        return true;
    }
    
	//@Override
	/**
	 * 
	 * @param url The path to the configuration file. If the file is not an absolute path, it will be searched
	 * in the current class path. Valid config parameters are
	 * <pre>
	 * {@code 
	 * driver=com.mysql.jdbc.Driver
	 * url=jdbc:mysql://localhost:3306/nafasi?autoReconnectForPools=true
	 * }
	 * </pre>
	 * @param properties the supplied driver specific properties for example
	 * <pre>
	 * {@code
	 * userid=nfsuser
	 * password=NaPHa5!
	 * }
	 * </pre>
	 * @return a valid {@link Connection} or null if a connection could not be made
	 */
	public Connection connect(String url, Properties properties) throws SQLException {
		try{
			String propertiesFileName = this.getConfigPath(url);
			if(propertiesFileName == null)
				return null;
			final Properties configProperties = initProperties(propertiesFileName);
			initConnectionPool(configProperties, properties);
			
			try {
				return poolManager.takeConnection();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}catch(Exception e){
			throw new SQLException(e);
		}
		return null;
	}

	@Override
	public int getMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub
		return false;
	}

	public static void main(String[] args){
		
	}
	
}