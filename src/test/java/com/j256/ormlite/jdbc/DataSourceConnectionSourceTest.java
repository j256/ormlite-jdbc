package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Test;

import com.j256.ormlite.BaseJdbcTest;
import com.j256.ormlite.db.H2DatabaseType;
import com.j256.ormlite.support.DatabaseConnection;

public class DataSourceConnectionSourceTest extends BaseJdbcTest {

	@Test
	public void testDscsUrl() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		expect(dataSource.getConnection()).andReturn(null);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource(dataSource, DEFAULT_DATABASE_URL);
		replay(dataSource);
		dcs.getReadOnlyConnection();
		dcs.close();
		verify(dataSource);
	}

	@Test
	public void testDscsSetUrl() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		Connection conn = createMock(Connection.class);
		conn.close();
		expect(dataSource.getConnection()).andReturn(conn);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.setDataSource(dataSource);
		dcs.setDatabaseUrl(DEFAULT_DATABASE_URL);
		dcs.initialize();
		replay(dataSource, conn);
		DatabaseConnection jdbcConn = dcs.getReadOnlyConnection();
		jdbcConn.close();
		dcs.close();
		verify(dataSource, conn);
	}

	@Test
	public void testDscsSetDatabaseType() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		expect(dataSource.getConnection()).andReturn(null);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.setDataSource(dataSource);
		dcs.setDatabaseUrl(DEFAULT_DATABASE_URL);
		dcs.setDatabaseType(new H2DatabaseType());
		dcs.initialize();
		replay(dataSource);
		dcs.getReadOnlyConnection();
		dcs.close();
		verify(dataSource);
	}

	@Test(expected = IllegalStateException.class)
	public void testDscsNoSetters() throws Exception {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.initialize();
		dcs.close();
	}

	@Test
	public void testDscsDoubleInit() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource(dataSource, connectionSource.getDatabaseType());
		dcs.initialize();
		assertEquals(connectionSource.getDatabaseType(), dcs.getDatabaseType());
		dcs.releaseConnection(createMock(DatabaseConnection.class));
		dcs.close();
	}

	@Test(expected = IllegalStateException.class)
	public void testDscsNoUrlOrType() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.setDataSource(dataSource);
		dcs.initialize();
		dcs.close();
	}

	@Test(expected = SQLException.class)
	public void testDscsGetReadOnlyNoInit() throws Exception {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.getReadOnlyConnection();
		dcs.close();
	}

	@Test(expected = SQLException.class)
	public void testDscsGetReadOnlyNoInitUP() throws Exception {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.getReadOnlyConnection("username", "password");
		dcs.close();
	}

	@Test(expected = SQLException.class)
	public void testDscsGetReadWriteNoInit() throws Exception {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.getReadWriteConnection();
		dcs.close();
	}

	@Test(expected = SQLException.class)
	public void testDscsGetReadWriteNoInitUP() throws Exception {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.getReadWriteConnection("username", "password");
		dcs.close();
	}

	@Test
	public void testDscsGetReadWrite() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		Connection conn = createMock(Connection.class);
		conn.close();
		expect(dataSource.getConnection()).andReturn(conn);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource(dataSource, DEFAULT_DATABASE_URL);
		replay(dataSource, conn);
		DatabaseConnection jdbcConn = dcs.getReadWriteConnection();
		jdbcConn.close();
		dcs.close();
		verify(dataSource, conn);
	}

	@Test
	public void testDscsGetReadWriteUP() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		Connection conn = createMock(Connection.class);
		conn.close();
		String userName = "user";
		String password = "password";
		expect(dataSource.getConnection(userName, password)).andReturn(conn);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource(dataSource, DEFAULT_DATABASE_URL);
		replay(dataSource, conn);
		DatabaseConnection jdbcConn = dcs.getReadOnlyConnection(userName, password);
		jdbcConn.close();
		dcs.close();
		verify(dataSource, conn);
	}

	@Test(expected = SQLException.class)
	public void testDscsReleaseNoInit() throws Exception {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.releaseConnection(createMock(DatabaseConnection.class));
		dcs.close();
	}

	@Test(expected = IOException.class)
	public void testDscsCloseNoInit() throws Exception {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.close();
	}

	@Test(expected = IllegalStateException.class)
	public void testDscsGetDatabaseTypeNoInit() throws Exception {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.getDatabaseType();
		dcs.close();
	}

	@Test
	public void testDscsGetReadOnlyAndClose() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		DataSourceConnectionSource dscs = new DataSourceConnectionSource(dataSource, new H2DatabaseType());
		Connection conn = createMock(Connection.class);
		conn.close();
		expect(dataSource.getConnection()).andReturn(conn);
		replay(dataSource, conn);
		DatabaseConnection dbConn = dscs.getReadOnlyConnection();
		dscs.releaseConnection(dbConn);
		dscs.close();
		verify(dataSource, conn);
	}
}
