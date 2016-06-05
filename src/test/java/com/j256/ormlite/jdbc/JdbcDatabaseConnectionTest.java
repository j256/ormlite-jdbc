package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.Test;

import com.j256.ormlite.BaseJdbcTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.stmt.GenericRowMapper;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.support.GeneratedKeyHolder;
import com.j256.ormlite.table.DatabaseTable;

public class JdbcDatabaseConnectionTest extends BaseJdbcTest {

	private static final String FOO_TABLE_NAME = "foo";
	private static final String FOOINT_TABLE_NAME = "fooint";

	@Test
	public void testQueryForLong() throws Exception {
		DatabaseConnection databaseConnection = connectionSource.getReadOnlyConnection(FOO_TABLE_NAME);
		try {
			Dao<Foo, Object> dao = createDao(Foo.class, true);
			Foo foo = new Foo();
			long id = 21321321L;
			foo.id = id;
			assertEquals(1, dao.create(foo));
			assertEquals(id, databaseConnection.queryForLong("select id from foo"));
		} finally {
			connectionSource.releaseConnection(databaseConnection);
		}
	}

	@Test(expected = SQLException.class)
	public void testQueryForLongNoResult() throws Exception {
		DatabaseConnection databaseConnection = connectionSource.getReadOnlyConnection(FOO_TABLE_NAME);
		try {
			createDao(Foo.class, true);
			databaseConnection.queryForLong("select id from foo");
		} finally {
			connectionSource.releaseConnection(databaseConnection);
		}
	}

	@Test(expected = SQLException.class)
	public void testQueryForLongTooManyResults() throws Exception {
		DatabaseConnection databaseConnection = connectionSource.getReadOnlyConnection(FOO_TABLE_NAME);
		try {
			Dao<Foo, Object> dao = createDao(Foo.class, true);
			Foo foo = new Foo();
			long id = 21321321L;
			foo.id = id;
			// insert twice
			assertEquals(1, dao.create(foo));
			assertEquals(1, dao.create(foo));
			databaseConnection.queryForLong("select id from foo");
		} finally {
			connectionSource.releaseConnection(databaseConnection);
		}
	}

	@Test
	public void testUpdateReleaseConnection() throws Exception {
		Connection connection = createMock(Connection.class);
		JdbcDatabaseConnection jdc = new JdbcDatabaseConnection(connection);
		String statement = "statement";
		PreparedStatement prepStmt = createMock(PreparedStatement.class);
		expect(connection.prepareStatement(statement)).andReturn(prepStmt);
		expect(prepStmt.executeUpdate()).andReturn(1);
		// should close the statement
		prepStmt.close();
		connection.close();
		replay(connection, prepStmt);
		jdc.update(statement, new Object[0], new FieldType[0]);
		jdc.close();
		verify(connection, prepStmt);
	}

	@Test
	public void testInsertReleaseConnection() throws Exception {
		Connection connection = createMock(Connection.class);
		PreparedStatement prepStmt = createMock(PreparedStatement.class);
		GeneratedKeyHolder keyHolder = createMock(GeneratedKeyHolder.class);
		ResultSet resultSet = createMock(ResultSet.class);
		ResultSetMetaData metaData = createMock(ResultSetMetaData.class);

		JdbcDatabaseConnection jdc = new JdbcDatabaseConnection(connection);
		String statement = "statement";
		expect(connection.prepareStatement(statement, 1)).andReturn(prepStmt);
		expect(prepStmt.executeUpdate()).andReturn(1);
		expect(prepStmt.getGeneratedKeys()).andReturn(resultSet);
		expect(resultSet.getMetaData()).andReturn(metaData);
		expect(resultSet.next()).andReturn(true);
		expect(metaData.getColumnCount()).andReturn(1);
		expect(metaData.getColumnType(1)).andReturn(Types.INTEGER);
		int keyHolderVal = 123131;
		expect(resultSet.getInt(1)).andReturn(keyHolderVal);
		keyHolder.addKey(keyHolderVal);
		expect(resultSet.next()).andReturn(false);
		// should close the statement
		prepStmt.close();
		connection.close();
		replay(connection, prepStmt, keyHolder, resultSet, metaData);
		jdc.insert(statement, new Object[0], new FieldType[0], keyHolder);
		jdc.close();
		verify(connection, prepStmt, keyHolder, resultSet, metaData);
	}

	@Test
	public void testQueryForOneReleaseConnection() throws Exception {
		Connection connection = createMock(Connection.class);
		PreparedStatement prepStmt = createMock(PreparedStatement.class);
		GeneratedKeyHolder keyHolder = createMock(GeneratedKeyHolder.class);
		ResultSet resultSet = createMock(ResultSet.class);
		@SuppressWarnings("unchecked")
		GenericRowMapper<Foo> rowMapper = createMock(GenericRowMapper.class);

		JdbcDatabaseConnection jdc = new JdbcDatabaseConnection(connection);
		String statement = "statement";
		expect(connection.prepareStatement(statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
				.andReturn(prepStmt);
		expect(prepStmt.executeQuery()).andReturn(resultSet);
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.next()).andReturn(false);
		resultSet.close();
		expect(prepStmt.getMoreResults()).andReturn(false);
		// should close the statement
		prepStmt.close();
		connection.close();
		replay(connection, prepStmt, keyHolder, resultSet, rowMapper);
		jdc.queryForOne(statement, new Object[0], new FieldType[0], rowMapper, null);
		jdc.close();
		verify(connection, prepStmt, keyHolder, resultSet, rowMapper);
	}

	@Test
	public void testQueryKeyHolderNoKeys() throws Exception {
		DatabaseConnection databaseConnection = connectionSource.getReadOnlyConnection(FOO_TABLE_NAME);
		try {
			createDao(Foo.class, true);
			GeneratedKeyHolder keyHolder = createMock(GeneratedKeyHolder.class);
			keyHolder.addKey(0L);
			replay(keyHolder);
			databaseConnection.insert("insert into foo (id) values (2)", new Object[0], new FieldType[0], keyHolder);
			verify(keyHolder);
		} finally {
			connectionSource.releaseConnection(databaseConnection);
		}
	}

	@Test
	public void testIdColumnInteger() throws Exception {
		// NOTE: this doesn't seem to generate an INTEGER type, oh well
		DatabaseConnection databaseConnection = connectionSource.getReadOnlyConnection(FOOINT_TABLE_NAME);
		try {
			createDao(FooInt.class, true);
			GeneratedKeyHolder keyHolder = createMock(GeneratedKeyHolder.class);
			keyHolder.addKey(1L);
			replay(keyHolder);
			databaseConnection.insert("insert into fooint (stuff) values (2)", new Object[0], new FieldType[0],
					keyHolder);
			verify(keyHolder);
		} finally {
			connectionSource.releaseConnection(databaseConnection);
		}
	}

	@Test
	public void testIdColumnInvalid() throws Exception {
		// NOTE: this doesn't seem to generate an INTEGER type, oh well
		DatabaseConnection databaseConnection = connectionSource.getReadOnlyConnection(FOOINT_TABLE_NAME);
		try {
			createDao(FooInt.class, true);
			GeneratedKeyHolder keyHolder = createMock(GeneratedKeyHolder.class);
			keyHolder.addKey(1L);
			replay(keyHolder);
			databaseConnection.insert("insert into fooint (stuff) values ('zipper')", new Object[0], new FieldType[0],
					keyHolder);
			verify(keyHolder);
		} finally {
			connectionSource.releaseConnection(databaseConnection);
		}
	}

	@Test
	public void testIdColumnChangedFromStringToNumber() throws Exception {
		// NOTE: trying to get the database to return a string as a result but could not figure it out
		DatabaseConnection databaseConnection = connectionSource.getReadOnlyConnection(FOOINT_TABLE_NAME);
		try {
			createDao(FooString.class, true);
			GeneratedKeyHolder keyHolder = createMock(GeneratedKeyHolder.class);
			keyHolder.addKey(0L);
			replay(keyHolder);
			databaseConnection.insert("insert into fooint (id, stuff) values ('12', 'zipper')", new Object[0],
					new FieldType[0], keyHolder);
			verify(keyHolder);
		} finally {
			connectionSource.releaseConnection(databaseConnection);
		}
	}

	@Test(expected = SQLException.class)
	public void testGeneratedIdNoReturn() throws Exception {
		createDao(FooNotGeneratedId.class, true);
		Dao<FooInt, Object> genDao = createDao(FooInt.class, false);
		FooInt foo = new FooInt();
		foo.stuff = "hello";
		genDao.create(foo);
	}

	/* =================================================================================================== */

	@DatabaseTable(tableName = FOO_TABLE_NAME)
	protected static class Foo {
		@DatabaseField
		public long id;

		Foo() {
		}
	}

	@DatabaseTable(tableName = FOOINT_TABLE_NAME)
	protected static class FooInt {
		@DatabaseField(generatedId = true)
		public int id;
		@DatabaseField
		public String stuff;

		FooInt() {
		}
	}

	@DatabaseTable(tableName = FOOINT_TABLE_NAME)
	protected static class FooString {
		@DatabaseField(id = true)
		public String id;
		@DatabaseField
		public String stuff;

		FooString() {
		}
	}

	@DatabaseTable(tableName = FOOINT_TABLE_NAME)
	protected static class FooNotGeneratedId {
		@DatabaseField
		public int id;
		@DatabaseField
		public String stuff;

		FooNotGeneratedId() {
		}
	}
}
