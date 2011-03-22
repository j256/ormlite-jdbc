package com.j256.ormlite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.j256.ormlite.TestUtils;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.table.TableInfo;

public class Db2DatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:db2:ormlitedb2";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new Db2DatabaseType();
	}

	@Override
	protected boolean isDriverClassExpected() {
		return false;
	}

	@Override
	@Test
	public void testEscapedEntityName() throws Exception {
		String word = "word";
		assertEquals("\"" + word + "\"", TestUtils.appendEscapedEntityName(databaseType, word));
	}

	@Test
	public void testBoolean() throws Exception {
		TableInfo<AllTypes, Void> tableInfo = new TableInfo<AllTypes, Void>(connectionSource, null, AllTypes.class);
		assertEquals(9, tableInfo.getFieldTypes().length);
		FieldType booleanField = tableInfo.getFieldTypes()[1];
		assertEquals("booleanField", booleanField.getDbColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, booleanField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("SMALLINT"));
	}

	@Test
	public void testByte() throws Exception {
		TableInfo<AllTypes, Void> tableInfo = new TableInfo<AllTypes, Void>(connectionSource, null, AllTypes.class);
		assertEquals(9, tableInfo.getFieldTypes().length);
		FieldType byteField = tableInfo.getFieldTypes()[3];
		assertEquals("byteField", byteField.getDbColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, byteField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("SMALLINT"));
	}

	@Test
	public void testGeneratedId() throws Exception {
		TableInfo<GeneratedId, Void> tableInfo =
				new TableInfo<GeneratedId, Void>(connectionSource, null, GeneratedId.class);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null, null);
		assertTrue(sb + "should contain the stuff", sb.toString().contains(" GENERATED ALWAYS AS IDENTITY"));
		assertEquals(0, statementsBefore.size());
		assertEquals(1, additionalArgs.size());
		assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
	}

	@Test
	public void testObject() throws Exception {
		Db2DatabaseType dbType = new Db2DatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendByteArrayType(sb);
		assertEquals("VARCHAR [] FOR BIT DATA", sb.toString());
	}

	@Test
	public void testUnique() throws Exception {
		StringBuilder sb = new StringBuilder();
		List<String> after = new ArrayList<String>();
		String fieldName = "id";
		Field field = Foo.class.getDeclaredField(fieldName);
		String tableName = "foo";
		FieldType fieldType = FieldType.createFieldType(connectionSource, tableName, field, Foo.class, 0);
		((BaseDatabaseType) databaseType).appendUnique(sb, fieldType, after);
		assertEquals(0, sb.length());
		assertEquals(1, after.size());
		assertEquals("ALTER TABLE \"" + tableName + "\" ADD UNIQUE (\"" + fieldName + "\");", after.get(0));
	}

	@Override
	@Test
	public void testOffsetSupport() throws Exception {
		assertFalse(databaseType.isOffsetSqlSupported());
	}
}
