package com.j256.ormlite.jdbc.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.j256.ormlite.TestUtils;
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
	public void testEscapedEntityName() {
		String word = "word";
		assertEquals("\"" + word + "\"", TestUtils.appendEscapedEntityName(databaseType, word));
	}

	@Test
	public void testDropSequence() throws Exception {
		Field field = GeneratedId.class.getField("id");
		FieldType fieldType = FieldType.createFieldType(databaseType, "foo", field, GeneratedId.class);
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
				new TableInfo<GeneratedIdSequence, Integer>(databaseType, GeneratedIdSequence.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null,
				null);
		assertEquals(1, statementsBefore.size());
		assertTrue(statementsBefore.get(0).contains(GENERATED_ID_SEQ.toUpperCase()),
				statementsBefore.get(0) + " should contain sequence");
		assertEquals(0, additionalArgs.size());
	}

	@Test
	public void testGeneratedIdSequenceAutoName() throws Exception {
		TableInfo<GeneratedIdSequenceAutoName, Integer> tableInfo =
				new TableInfo<GeneratedIdSequenceAutoName, Integer>(databaseType, GeneratedIdSequenceAutoName.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		FieldType idField = tableInfo.getFieldTypes()[0];
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, idField, additionalArgs, statementsBefore, null, null);
		assertEquals(1, statementsBefore.size());
		String seqName = databaseType
				.generateIdSequenceName(GeneratedIdSequenceAutoName.class.getSimpleName().toLowerCase(), idField);
		assertTrue(statementsBefore.get(0).contains(seqName));
		assertEquals(0, additionalArgs.size());
	}

	@Test
	public void testByte() throws Exception {
		TableInfo<AllTypes, Integer> tableInfo = new TableInfo<AllTypes, Integer>(databaseType, AllTypes.class);
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
	public void testLong() throws Exception {
		TableInfo<AllTypes, Integer> tableInfo = new TableInfo<AllTypes, Integer>(databaseType, AllTypes.class);
		assertEquals(9, tableInfo.getFieldTypes().length);
		FieldType booleanField = tableInfo.getFieldTypes()[6];
		assertEquals("longField", booleanField.getColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, booleanField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("NUMERIC"));
	}

	@Test
	public void testObject() {
		OracleDatabaseType dbType = new OracleDatabaseType();
		StringBuilder sb = new StringBuilder();
		dbType.appendByteArrayType(sb, null, 0);
		assertEquals("LONG RAW", sb.toString());
	}

	@Test
	public void testSelectNextVal() {
		OracleDatabaseType dbType = new OracleDatabaseType();
		StringBuilder sb = new StringBuilder();
		String sequenceName = "stuff_seq";
		dbType.appendSelectNextValFromSequence(sb, sequenceName);
		assertEquals("SELECT \"" + sequenceName + "\".nextval FROM dual", sb.toString());
	}

	@Override
	@Test
	public void testOffsetSupport() {
		assertFalse(databaseType.isOffsetSqlSupported());
	}
}
