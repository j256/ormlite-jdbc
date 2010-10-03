package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import com.j256.ormlite.BaseOrmLiteCoreTest;

public class JdbcConnectionSourceTest extends BaseOrmLiteCoreTest {

	@Test
	public void testSimpleDataSource() {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		String url = "foo:bar:baz";
		sds.setUrl(url);
		assertEquals(url, sds.getUrl());
	}

	@Test
	public void testSimpleDataSourceString() throws Exception {
		String url = "jdbc:h2:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url);
		assertEquals(url, sds.getUrl());
	}

	@Test
	public void testSimpleDataSourceStringStringString() throws Exception {
		String username = "user";
		String password = "_secret";
		String url = "jdbc:h2:ormlite-up;USER=" + username + ";PASSWORD=" + password;
		JdbcConnectionSource sds = new JdbcConnectionSource(url, username, password);
		assertNotNull(sds.getReadOnlyConnection());
	}

	@Test
	public void testGetConnection() throws Exception {
		String url = "jdbc:bar:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
		Connection conn = createMock(Connection.class);
		Driver driver = createMock(Driver.class);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver);
		DriverManager.registerDriver(driver);
		try {
			assertNotNull(sds.getReadOnlyConnection());
			verify(driver);
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testGetConnectionUserPassSetters() throws Exception {
		String username = "user";
		String password = "_secret";
		String url = "jdbc:h2:mem:ormlite-up;USER=" + username + ";PASSWORD=" + password;
		JdbcConnectionSource sds = new JdbcConnectionSource(url);
		sds.setUsername(username);
		sds.setPassword(password);
		assertNotNull(sds.getReadOnlyConnection());
	}

	@Test(expected = SQLException.class)
	public void testGetConnectionNull() throws Exception {
		String url = "jdbc:bar:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
		Driver driver = createMock(Driver.class);
		Properties props = new Properties();
		expect(driver.connect(eq(url), eq(props))).andReturn(null);
		replay(driver);
		DriverManager.registerDriver(driver);
		try {
			sds.getReadOnlyConnection();
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testClose() throws Exception {
		String url = "jdbc:bar:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
		Connection conn = createMock(Connection.class);
		conn.close();
		Driver driver = createMock(Driver.class);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver);
		replay(conn);
		DriverManager.registerDriver(driver);
		try {
			assertNotNull(sds.getReadOnlyConnection());
			sds.close();
			verify(driver);
			verify(conn);
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test(expected = SQLException.class)
	public void testInitNoUrl() throws Exception {
		new JdbcConnectionSource().initialize();
	}

	@Test(expected = SQLException.class)
	public void testConnectionClosed() throws Exception {
		String url = "jdbc:bar:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
		Connection conn = createMock(Connection.class);
		expect(conn.isClosed()).andReturn(true);
		Driver driver = createMock(Driver.class);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver);
		replay(conn);
		DriverManager.registerDriver(driver);
		try {
			assertNotNull(sds.getReadOnlyConnection());
			sds.getReadOnlyConnection();
			fail("Should not get here");
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testSpringWiring() throws Exception {
		String url = "jdbc:h2:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.setUrl(url);
		sds.initialize();
	}
}
