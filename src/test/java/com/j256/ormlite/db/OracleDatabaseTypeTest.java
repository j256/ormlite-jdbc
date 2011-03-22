package com.j256.ormlite.db;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.j256.ormlite.TestUtils;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.table.TableInfo;

public class OracleDatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:oracle:ormliteoracle";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new OracleDatabaseType();
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

	@Test(expected = IllegalStateException.class)
	public void testBadGeneratedId() throws Exception {
		Field field = GeneratedId.class.getField("id");
		DatabaseType mockDb = createMock(DatabaseType.class);
		expect(mockDb.isIdSequenceNeeded()).andReturn(false);
		expect(mockDb.getFieldConverter(isA(DataType.class))).andReturn(null);
		expect(mockDb.isEntityNamesMustBeUpCase()).andReturn(false);
		replay(mockDb);
		connectionSource.setDatabaseType(mockDb);
		try {
			FieldType fieldType = FieldType.createFieldType(connectionSource, "foo", field, GeneratedId.class, 0);
			verify(mockDb);
			StringBuilder sb = new StringBuilder();
			List<String> statementsBefore = new ArrayList<String>();
			databaseType.appendColumnArg(sb, fieldType, null, statementsBefore, null, null);
		} finally {
			connectionSource.setDatabaseType(databaseType);
		}
	}

	@Test
	public void testDropSequence() throws Exception {
		Field field = GeneratedId.class.getField("id");
		FieldType fieldType = FieldType.createFieldType(connectionSource, "foo", field, GeneratedId.class, 0);
		List<String> statementsBefore = new ArrayList<String>();
		List<String> statementsAfter = new ArrayList<String>();
		databaseType.dropColumnArg(fieldType, statementsBefore, statementsAfter);
		assertEquals(0, statementsBefore.size());
		assertEquals(1, statementsAfter.size());
		assertTrue(statementsAfter.get(0).contains("DROP SEQUENCE "));
	}

	@Test
	@Override
	public void testGeneratedIdSequence() throws Exception {
		TableInfo<GeneratedIdSequence, Integer> tableInfo =
				new TableInfo<GeneratedIdSequence, Integer>(connectionSource, null, GeneratedIdSequence.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains(" PRIMARY KEY"));
		assertEquals(1, statementsBefore.size());
		assertTrue(statementsBefore.get(0).contains(GENERATED_ID_SEQ));
		assertEquals(0, additionalArgs.size());
	}

	@Test
	public void testGeneratedIdSequenceAutoName() throws Exception {
		TableInfo<GeneratedIdSequenceAutoName, Integer> tableInfo =
				new TableInfo<GeneratedIdSequenceAutoName, Integer>(connectionSource, null,
						GeneratedIdSequenceAutoName.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		FieldType idField = tableInfo.getFieldTypes()[0];
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, idField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains(" PRIMARY KEY"));
		assertEquals(1, statementsBefore.size());
		String seqName =
				databaseType.generateIdSequenceName(GeneratedIdSequenceAutoName.class.getSimpleName().toLowerCase(),
						idField);
		assertTrue(statementsBefore.get(0).contains(seqName));
		assertEquals(0, additionalArgs.size());
	}

	@Test
	public void testByte() throws Exception {
		TableInfo<AllTypes, Integer> tableInfo =
				new TableInfo<AllTypes, Integer>(connectionSource, null, AllTypes.class);
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
	public void testLong() throws Exception {
		TableInfo<AllTypes, Integer> tableInfo =
				new TableInfo<AllTypes, Integer>(connectionSource, null, AllTypes.class);
		assertEquals(9, tableInfo.getFieldTypes().length);
		FieldType booleanField = tableInfo.getFieldTypes()[6];
		assertEquals("longField", booleanField.getDbColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, booleanField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("NUMERIC"));
	}

	@Test
	public void testUnique() throws Exception {
		OracleDatabaseType dbType = new OracleDatabaseType();
		StringBuilder sb = new StringBuilder();
		List<String> after = new ArrayList<String>();
		String fieldName = "id";
		Field field = Foo.class.getDeclaredField(fieldName);
		String tableName = "foo";
		connectionSource.setDatabaseType(dbType);
		try {
			FieldType fieldType = FieldType.createFieldType(connectionSource, tableName, field, Foo.class, 0);
			dbType.appendUnique(sb, fieldType, after);
			assertEquals(0, sb.length());
			assertEquals(1, after.size());
			assertEquals("ALTER TABLE \"" + tableName + "\" ADD CONSTRAINT " + tableName + "_" + fieldName
					+ "_unique UNIQUE (\"" + fieldName + "\");", after.get(0));
		} finally {
			connectionSource.setDatabaseType(databaseType);
		}
	}

	@Test
	public void testObject() throws Exception {
		OracleDatabaseType dbType = new OracleDatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendByteArrayType(sb);
		assertEquals("LONG RAW", sb.toString());
	}

	@Test
	public void testSelectNextVal() throws Exception {
		OracleDatabaseType dbType = new OracleDatabaseType();
		StringBuilder sb = new StringBuilder();
		String sequenceName = "stuff_seq";
		dbType.appendSelectNextValFromSequence(sb, sequenceName);
		assertEquals("SELECT \"" + sequenceName + "\".nextval FROM dual", sb.toString());
	}

	@Override
	@Test
	public void testOffsetSupport() throws Exception {
		assertFalse(databaseType.isOffsetSqlSupported());
	}
}
