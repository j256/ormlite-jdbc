package com.j256.ormlite.jdbc;

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Test;

import com.j256.ormlite.BaseCoreTest;
import com.j256.ormlite.dao.Dao;

public class JdbcSingleConnectionSourceTest extends BaseCoreTest {

	private static final String DEFAULT_DATABASE_URL = "jdbc:h2:mem:ormlite";

	@Test
	public void testStuff() throws SQLException {
		Connection connection = DriverManager.getConnection(DEFAULT_DATABASE_URL);
		JdbcSingleConnectionSource connectionSource = new JdbcSingleConnectionSource(DEFAULT_DATABASE_URL, connection);
		try {
			Dao<Foo, Integer> dao = createDao(connectionSource, Foo.class, true);
			Foo foo = new Foo();
			dao.create(foo);
			Foo result = dao.queryForId(foo.id);
			assertNotNull(result);
		} finally {
			connectionSource.close();
			connection.close();
		}
	}
}
