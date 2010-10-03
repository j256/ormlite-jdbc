package com.j256.ormlite.db;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Constructor;
import java.sql.SQLException;

import org.junit.Test;

import com.j256.ormlite.support.ConnectionSource;

public class DatabaseTypeUtilsTest {

	@Test
	public void testConstructor() throws Exception {
		@SuppressWarnings("unchecked")
		Constructor[] constructors = DatabaseTypeUtils.class.getDeclaredConstructors();
		assertEquals(1, constructors.length);
		constructors[0].setAccessible(true);
		constructors[0].newInstance();
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testLoadDriver() throws Exception {
		DatabaseTypeUtils.loadDriver("jdbc:h2:mem:ormlitetest");
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testSimpleDataSource() throws Exception {
		DatabaseTypeUtils.createJdbcConnectionSource("jdbc:h2:mem:ormlitetest").getReadOnlyConnection().close();
	}

	@SuppressWarnings("deprecation")
	@Test(expected = SQLException.class)
	public void testSimpleDataSourceBadDriverArgs() throws Exception {
		DatabaseTypeUtils.createJdbcConnectionSource("jdbc:h2:").getReadOnlyConnection();
	}

	@Test
	public void testCreateDbType() throws Exception {
		DatabaseTypeUtils.createDatabaseType("jdbc:h2:mem:ormlitetest");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreateDbTypeBadDriver() throws Exception {
		DatabaseTypeUtils.createDatabaseType("jdbc:unknown-db:");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreateDbTypeBadUrl() throws Exception {
		DatabaseTypeUtils.createDatabaseType("bad-url");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreateDbTypeNotEnoughParts() throws Exception {
		DatabaseTypeUtils.createDatabaseType("jdbc:");
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testCreateDbTypeDataSource() throws Exception {
		ConnectionSource dataSource = null;
		try {
			String dbUrl = "jdbc:h2:mem:ormlitetest";
			dataSource = DatabaseTypeUtils.createJdbcConnectionSource(dbUrl);
			DatabaseTypeUtils.createDatabaseType(dbUrl);
		} finally {
			if (dataSource != null) {
				dataSource.getReadOnlyConnection().close();
			}
		}
	}
}
