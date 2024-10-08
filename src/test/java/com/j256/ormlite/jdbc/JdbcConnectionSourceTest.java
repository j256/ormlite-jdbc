package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.j256.ormlite.BaseCoreTest;
import com.j256.ormlite.jdbc.db.H2DatabaseType;
import com.j256.ormlite.support.DatabaseConnection;

public class JdbcConnectionSourceTest extends BaseCoreTest {

	@Test
	public void testSimpleDataSource() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		String url = "jdbc:h2:mem:baz";
		sds.setUrl(url);
		assertEquals(url, sds.getUrl());
		sds.initialize();
		sds.close();
	}

	@Test
	public void testSimpleDataSourceString() throws Exception {
		String url = "jdbc:h2:mem:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url);
		assertEquals(url, sds.getUrl());
		sds.close();
	}

	@Test
	public void testSimpleDataSourceStringStringString() throws Exception {
		String username = "user";
		String password = "_secret";
		String url = "jdbc:h2:mem:ormlite-up;USER=" + username + ";PASSWORD=" + password;
		JdbcConnectionSource sds = new JdbcConnectionSource(url, username, password);
		assertNotNull(sds.getReadOnlyConnection(null));
		sds.close();
	}

	@Test
	public void testGetConnection() throws Exception {
		Connection conn = createMock(Connection.class);
		Driver driver = createMock(Driver.class);
		String url = "jdbc:bar:mem:baz";
		expect(driver.acceptsURL(url)).andReturn(true);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver);
		DriverManager.registerDriver(driver);
		try {
			JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
			assertNotNull(sds.getReadOnlyConnection(null));
			sds.close();
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
		assertNotNull(sds.getReadOnlyConnection(null));
		sds.close();
	}

	@Test
	public void testGetConnectionNull() throws Exception {
		Driver driver = createMock(Driver.class);
		Properties props = new Properties();
		String url = "jdbc:bar:baz";
		expect(driver.acceptsURL(url)).andReturn(true);
		expect(driver.connect(eq(url), eq(props))).andReturn(null);
		replay(driver);
		DriverManager.registerDriver(driver);
		try {
			JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
			assertThrowsExactly(SQLException.class, () -> {
				sds.getReadOnlyConnection(null);
				sds.close();
			});
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testClose() throws Exception {
		Connection conn = createMock(Connection.class);
		conn.setAutoCommit(true);
		conn.close();
		Driver driver = createMock(Driver.class);
		String url = "jdbc:bar:baz";
		expect(driver.acceptsURL(url)).andReturn(true);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver, conn);
		DriverManager.registerDriver(driver);
		try {
			JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
			assertNotNull(sds.getReadOnlyConnection(null));
			sds.close();
			verify(driver, conn);
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testInitNoUrl() {
		JdbcConnectionSource cs = new JdbcConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			cs.initialize();
			cs.close();
		});
	}

	@Test
	public void testConnectionClosed() throws Exception {
		Connection conn = createMock(Connection.class);
		conn.setAutoCommit(true);
		expect(conn.isClosed()).andReturn(true);
		Driver driver = createMock(Driver.class);
		String url = "jdbc:bar:baz";
		expect(driver.acceptsURL(url)).andReturn(true);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver, conn);
		DriverManager.registerDriver(driver);
		try {
			JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
			assertNotNull(sds.getReadOnlyConnection(null));
			assertThrowsExactly(SQLException.class, () -> {
				sds.getReadOnlyConnection(null);
				sds.close();
			});
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testSpringWiring() throws Exception {
		String url = "jdbc:h2:mem:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.setUrl(url);
		sds.initialize();
		sds.close();
	}

	@Test
	public void testCloseBeforeInitialize() {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			sds.close();
		});
	}

	@Test
	public void testGetReadOnlyConnectionBeforeInitialize() {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			sds.getReadOnlyConnection(null);
			sds.close();
		});
	}

	@Test
	public void testGetReadWriteConnectionBeforeInitialize() {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			sds.getReadWriteConnection(null);
			sds.close();
		});
	}

	@Test
	public void testReleaseConnectionBeforeInitialize() {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		assertThrowsExactly(SQLException.class, () -> {
			sds.releaseConnection(null);
			sds.close();
		});
	}

	@Test
	public void testGetDatabaseTypeBeforeInitialize() {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		assertThrowsExactly(IllegalStateException.class, () -> {
			sds.getDatabaseType();
			sds.close();
		});
	}

	@Test
	public void testDoubleInit() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource("jdbc:h2:mem:baz");
		sds.initialize();
		sds.close();
	}

	@Test
	public void testSaveAndClear() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource("jdbc:h2:mem:baz");
		DatabaseConnection conn1 = sds.getReadOnlyConnection(null);
		DatabaseConnection conn2 = sds.getReadOnlyConnection(null);
		assertSame(conn1, conn2);
		sds.saveSpecialConnection(conn1);
		sds.clearSpecialConnection(conn1);
		sds.releaseConnection(conn1);
		sds.close();
	}

	@Test
	public void testSetDatabaseType() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.setUrl("jdbc:h2:mem:baz");
		sds.setDatabaseType(new H2DatabaseType());
		sds.initialize();
		assertTrue(sds.getDatabaseType() instanceof H2DatabaseType);
		sds.close();
	}

	@Test
	public void testIsOpen() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource("jdbc:h2:mem:baz");
		// no get connection yet
		assertFalse(sds.isOpen(null));
		sds.releaseConnection(sds.getReadOnlyConnection(null));
		assertTrue(sds.isOpen(null));
		sds.close();
		assertFalse(sds.isOpen(null));
	}
}
