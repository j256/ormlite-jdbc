package com.j256.ormlite.jdbc.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

public class PostgresDatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:postgresql:ormlitepostgres";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new PostgresDatabaseType();
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
	public void testEscapedEntityNameSchema() {
		String schema = "schema";
		String table = "table";
		String word = schema + "." + table;
		assertEquals("\"" + schema + "\".\"" + table + "\"", TestUtils.appendEscapedEntityName(databaseType, word));
	}

	@Test
	public void testDropSequence() throws Exception {
		if (connectionSource == null) {
			return;
		}
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
		if (connectionSource == null) {
			return;
		}
		TableInfo<GeneratedIdSequence, Integer> tableInfo =
				new TableInfo<GeneratedIdSequence, Integer>(databaseType, GeneratedIdSequence.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		List<String> queriesAfter = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null,
				queriesAfter);
		databaseType.addPrimaryKeySql(tableInfo.getFieldTypes(), additionalArgs, statementsBefore, null, queriesAfter);
		assertTrue(sb.toString().contains(" DEFAULT NEXTVAL('\"" + GENERATED_ID_SEQ + "\"')"));
		assertEquals(1, statementsBefore.size());
		assertTrue(statementsBefore.get(0).contains(GENERATED_ID_SEQ));
		assertEquals(1, additionalArgs.size());
		assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
		assertEquals(0, queriesAfter.size());
	}

	@Test
	public void testGeneratedIdSequenceAutoName() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TableInfo<GeneratedIdSequenceAutoName, Integer> tableInfo =
				new TableInfo<GeneratedIdSequenceAutoName, Integer>(databaseType, GeneratedIdSequenceAutoName.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		FieldType idField = tableInfo.getFieldTypes()[0];
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		List<String> queriesAfter = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, idField, additionalArgs, statementsBefore, null, queriesAfter);
		databaseType.addPrimaryKeySql(new FieldType[] { idField }, additionalArgs, statementsBefore, null,
				queriesAfter);
		String seqName = databaseType
				.generateIdSequenceName(GeneratedIdSequenceAutoName.class.getSimpleName().toLowerCase(), idField);
		assertTrue(sb.toString().contains(" DEFAULT NEXTVAL('\"" + seqName + "\"')"));
		assertEquals(1, statementsBefore.size());
		assertTrue(statementsBefore.get(0).contains(seqName));
		assertEquals(1, additionalArgs.size());
		assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
		assertEquals(0, queriesAfter.size());
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
		assertTrue(sb.toString().contains("BOOLEAN"));
	}

	@Test
	public void testByte() throws Exception {
		if (connectionSource == null) {
			return;
		}
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
}
