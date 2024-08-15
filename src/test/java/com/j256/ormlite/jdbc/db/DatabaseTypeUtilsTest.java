package com.j256.ormlite.jdbc.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.lang.reflect.Constructor;

import org.junit.jupiter.api.Test;

import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.support.ConnectionSource;

public class DatabaseTypeUtilsTest {

	@Test
	public void testConstructor() throws Exception {
		@SuppressWarnings("rawtypes")
		Constructor[] constructors = DatabaseTypeUtils.class.getDeclaredConstructors();
		assertEquals(1, constructors.length);
		constructors[0].setAccessible(true);
		constructors[0].newInstance();
	}

	@Test
	public void testCreateDbType() {
		DatabaseTypeUtils.createDatabaseType("jdbc:h2:mem:ormlitetest");
	}

	@Test
	public void testCreateDbTypeBadDriver() {
		assertThrowsExactly(IllegalArgumentException.class, () -> {
			DatabaseTypeUtils.createDatabaseType("jdbc:unknown-db:");
		});
	}

	@Test
	public void testCreateDbTypeBadUrl() {
		assertThrowsExactly(IllegalArgumentException.class, () -> {
			DatabaseTypeUtils.createDatabaseType("bad-url");
		});
	}

	@Test
	public void testCreateDbTypeNotEnoughParts() {
		assertThrowsExactly(IllegalArgumentException.class, () -> {
			DatabaseTypeUtils.createDatabaseType("jdbc:");
		});
	}

	@Test
	public void testCreateDbTypeDataSource() throws Exception {
		ConnectionSource dataSource = null;
		try {
			String dbUrl = "jdbc:h2:mem:ormlitetest";
			dataSource = new JdbcConnectionSource(dbUrl, new H2DatabaseType());
			dataSource.close();
		} finally {
			if (dataSource != null) {
				dataSource.getReadOnlyConnection(null).close();
			}
		}
	}
}
