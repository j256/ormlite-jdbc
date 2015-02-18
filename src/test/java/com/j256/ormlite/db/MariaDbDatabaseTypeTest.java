package com.j256.ormlite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.table.TableInfo;

public class MariaDbDatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:mariadb:ormlite";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new MariaDbDatabaseType();
	}

	@Override
	protected boolean isDriverClassExpected() {
		return false;
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
		assertTrue(sb.toString().contains("TINYINT(1)"));
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
		assertTrue(sb.toString().contains(" AUTO_INCREMENT"));
		assertEquals(1, additionalArgs.size());
		assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
	}

	@Test
	public void testTableSuffix() {
		MariaDbDatabaseType dbType = new MariaDbDatabaseType();
		String suffix = "ewfwefef";
		dbType.setCreateTableSuffix(suffix);
		StringBuilder sb = new StringBuilder();
		dbType.appendCreateTableSuffix(sb);
		assertTrue(sb.toString().contains(suffix));
	}

	@Test
	public void testDateTime() {
		MariaDbDatabaseType dbType = new MariaDbDatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendDateType(sb, null, 0);
		assertEquals("DATETIME", sb.toString());
	}

	@Test
	public void testObject() {
		MariaDbDatabaseType dbType = new MariaDbDatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendByteArrayType(sb, null, 0);
		assertEquals("BLOB", sb.toString());
	}

	@Test
	public void testLongStringSchema() {
		MariaDbDatabaseType dbType = new MariaDbDatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendLongStringType(sb, null, 0);
		assertEquals("TEXT", sb.toString());
	}
}
