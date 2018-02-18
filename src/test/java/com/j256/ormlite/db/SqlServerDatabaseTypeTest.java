package com.j256.ormlite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.j256.ormlite.TestUtils;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.table.TableInfo;

public class SqlServerDatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:sqlserver:db";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new SqlServerDatabaseType();
	}

	@Override
	protected boolean isDriverClassExpected() {
		return false;
	}

	@Override
	@Test
	public void testEscapedEntityName() {
		String word = "word";
		assertEquals("[" + word + "]", TestUtils.appendEscapedEntityName(databaseType, word));
	}

	@Test
	public void testMultipartEscapedEntityName() {
		String firstPart = "firstPart";
		String secondPart = "secondPart";
		String input = firstPart + "." + secondPart;
		String expected = "[" + firstPart + "].[" + secondPart + "]";
		assertEquals(expected, TestUtils.appendEscapedEntityName(databaseType, input));
	}

	@Override
	@Test
	public void testLimitAfterSelect() {
		assertTrue(databaseType.isLimitAfterSelect());
	}

	@Override
	@Test
	public void testLimitFormat() throws Exception {
		if (connectionSource == null) {
			return;
		}
		Dao<StringId, String> dao;
		try {
			connectionSource.setDatabaseType(databaseType);
			dao = createDao(StringId.class, false);
		} finally {
			connectionSource.setDatabaseType(new H2DatabaseType());
		}
		QueryBuilder<StringId, String> qb = dao.queryBuilder();
		long limit = 1232;
		qb.limit(limit);
		String query = qb.prepareStatementString();
		assertTrue(query + " should start with stuff", query.startsWith("SELECT TOP " + limit + " "));
	}

	@Override
	@Test
	public void testOffsetSupport() {
		assertFalse(databaseType.isOffsetSqlSupported());
	}

	@Test
	public void testBoolean() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TableInfo<AllTypes, Integer> tableInfo =
				new TableInfo<AllTypes, Integer>(connectionSource, null, AllTypes.class);
		assertEquals(9, tableInfo.getFieldTypes().length);
		FieldType booleanField = tableInfo.getFieldTypes()[1];
		assertEquals("booleanField", booleanField.getColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, booleanField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("BIT"));
	}

	@Test
	public void testByte() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TableInfo<AllTypes, Integer> tableInfo =
				new TableInfo<AllTypes, Integer>(connectionSource, null, AllTypes.class);
		assertEquals(9, tableInfo.getFieldTypes().length);
		FieldType byteField = tableInfo.getFieldTypes()[3];
		assertEquals("byteField", byteField.getColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, byteField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("SMALLINT"));
	}

	@Test
	public void testDate() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TableInfo<AllTypes, Integer> tableInfo =
				new TableInfo<AllTypes, Integer>(connectionSource, null, AllTypes.class);
		assertEquals(9, tableInfo.getFieldTypes().length);
		FieldType byteField = tableInfo.getFieldTypes()[2];
		assertEquals("dateField", byteField.getColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, byteField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("DATETIME"));
	}

	@Test
	public void testGeneratedIdBuilt() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TableInfo<GeneratedId, Integer> tableInfo =
				new TableInfo<GeneratedId, Integer>(connectionSource, null, GeneratedId.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null,
				null);
		databaseType.addPrimaryKeySql(tableInfo.getFieldTypes(), additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains(" IDENTITY"));
		assertEquals(1, additionalArgs.size());
		assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
	}
}
