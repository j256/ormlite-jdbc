package com.j256.ormlite.jdbc.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.table.TableInfo;

public class MysqlDatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:mysql:ormlite";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new MysqlDatabaseType();
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
		TableInfo<AllTypes, Integer> tableInfo = new TableInfo<AllTypes, Integer>(databaseType, AllTypes.class);
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
				new TableInfo<GeneratedId, Integer>(databaseType, GeneratedId.class);
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
		MysqlDatabaseType dbType = new MysqlDatabaseType();
		String suffix = "ewfwefef";
		dbType.setCreateTableSuffix(suffix);
		StringBuilder sb = new StringBuilder();
		dbType.appendCreateTableSuffix(sb);
		assertTrue(sb.toString().contains(suffix));
	}

	@Test
	public void testDateTime() {
		MysqlDatabaseType dbType = new MysqlDatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendDateType(sb, null, 0);
		assertEquals("DATETIME", sb.toString());
	}

	@Test
	public void testObject() {
		MysqlDatabaseType dbType = new MysqlDatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendByteArrayType(sb, null, 0);
		assertEquals("BLOB", sb.toString());
	}

	@Test
	public void testLongStringSchema() {
		MysqlDatabaseType dbType = new MysqlDatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendLongStringType(sb, null, 0);
		assertEquals("TEXT", sb.toString());
	}
}
