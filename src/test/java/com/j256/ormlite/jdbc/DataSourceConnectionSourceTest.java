package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import com.j256.ormlite.jdbc.db.H2DatabaseType;
import com.j256.ormlite.support.DatabaseConnection;

public class DataSourceConnectionSourceTest extends BaseJdbcTest {

	private static final String TABLE_NAME = "sometable";

	@Test
	public void testDscsUrl() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(null);
		replay(dataSource);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource(dataSource, DEFAULT_DATABASE_URL);
		dcs.getReadOnlyConnection(TABLE_NAME);
		dcs.close();
		verify(dataSource);
	}

	@Test
	public void testDscsSetUrl() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		Connection conn = createMock(Connection.class);
		conn.setAutoCommit(true);
		conn.setAutoCommit(true);
		conn.setAutoCommit(false);
		expect(conn.getAutoCommit()).andReturn(false);
		conn.setAutoCommit(true);
		conn.close();
		conn.close();
		conn.close();
		expect(dataSource.getConnection()).andReturn(conn);
		expect(dataSource.getConnection()).andReturn(conn);
		expect(dataSource.getConnection()).andReturn(conn);
		replay(dataSource, conn);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.setDataSource(dataSource);
		dcs.setDatabaseUrl(DEFAULT_DATABASE_URL);
		dcs.initialize();
		DatabaseConnection jdbcConn = dcs.getReadOnlyConnection(TABLE_NAME);
		jdbcConn.close();
		dcs.close();
		verify(dataSource, conn);
	}

	@Test
	public void testDscsSetDatabaseType() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(null);
		replay(dataSource);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.setDataSource(dataSource);
		dcs.setDatabaseUrl(DEFAULT_DATABASE_URL);
		dcs.setDatabaseType(new H2DatabaseType());
		dcs.initialize();
		dcs.getReadOnlyConnection(TABLE_NAME);
		dcs.close();
		verify(dataSource);
	}

	@Test
	public void testDscsNoSetters() {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		assertThrowsExactly(IllegalStateException.class, () -> {
			dcs.initialize();
			dcs.close();
		});
	}

	@Test
	public void testDscsDoubleInit() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(null);
		replay(dataSource);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource(dataSource, connectionSource.getDatabaseType());
		dcs.initialize();
		assertEquals(connectionSource.getDatabaseType(), dcs.getDatabaseType());
		dcs.releaseConnection(createMock(DatabaseConnection.class));
		dcs.close();
		verify(dataSource);
	}

	@Test
	public void testDscsNoUrlOrType() {
		DataSource dataSource = createMock(DataSource.class);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		dcs.setDataSource(dataSource);
		assertThrowsExactly(IllegalStateException.class, () -> {
			dcs.initialize();
			dcs.close();
		});
	}

	@Test
	public void testDscsGetReadOnlyNoInit() {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			dcs.getReadOnlyConnection(TABLE_NAME);
			dcs.close();
		});
	}

	@Test
	public void testDscsGetReadOnlyNoInitUP() {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			dcs.getReadOnlyConnection(TABLE_NAME, "username", "password");
			dcs.close();
		});
	}

	@Test
	public void testDscsGetReadWriteNoInit() {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			dcs.getReadWriteConnection(TABLE_NAME);
			dcs.close();
		});
	}

	@Test
	public void testDscsGetReadWriteNoInitUP() {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			dcs.getReadWriteConnection(TABLE_NAME, "username", "password");
			dcs.close();
		});
	}

	@Test
	public void testDscsGetReadWrite() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		Connection conn = createMock(Connection.class);
		conn.close();
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(conn);
		replay(dataSource, conn);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource(dataSource, DEFAULT_DATABASE_URL);
		DatabaseConnection jdbcConn = dcs.getReadWriteConnection(TABLE_NAME);
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
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection(userName, password)).andReturn(conn);
		replay(dataSource, conn);
		DataSourceConnectionSource dcs = new DataSourceConnectionSource(dataSource, DEFAULT_DATABASE_URL);
		DatabaseConnection jdbcConn = dcs.getReadOnlyConnection(TABLE_NAME, userName, password);
		jdbcConn.close();
		dcs.close();
		verify(dataSource, conn);
	}

	@Test
	public void testDscsReleaseNoInit() {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			dcs.releaseConnection(createMock(DatabaseConnection.class));
			dcs.close();
		});
	}

	@Test
	public void testDscsCloseNoInit() {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			dcs.close();
		});
	}

	@Test
	public void testDscsGetDatabaseTypeNoInit() {
		DataSourceConnectionSource dcs = new DataSourceConnectionSource();
		assertThrowsExactly(IllegalStateException.class, () -> {
			dcs.getDatabaseType();
			dcs.close();
		});
	}

	@Test
	public void testDscsGetReadOnlyAndClose() throws Exception {
		DataSource dataSource = createMock(DataSource.class);
		Connection conn = createMock(Connection.class);
		conn.close();
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(null);
		expect(dataSource.getConnection()).andReturn(conn);
		replay(dataSource, conn);
		DataSourceConnectionSource dscs = new DataSourceConnectionSource(dataSource, new H2DatabaseType());
		DatabaseConnection dbConn = dscs.getReadOnlyConnection(TABLE_NAME);
		dscs.releaseConnection(dbConn);
		dscs.close();
		verify(dataSource, conn);
	}
}
