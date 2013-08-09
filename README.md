mycp.java
=========

A Simple Java Connection Pool

mycp.java aims to be a very simple connection pool initially for JDBC but could extend to other concepts such as JMS.

Objectives

1. Only depends on the core standard Java API

2. Can be used in any application with a single connection string such as 
	String url = "jdbc:kakooge:<file-system-path-to>/mycp.properties";
	Class.forName("org.kakooge.mycp.JdbcDriver");

3. The connection string in 2. above can be used either directly using DriverManager.getConnection(...) or using Hibernate or any other database tool
