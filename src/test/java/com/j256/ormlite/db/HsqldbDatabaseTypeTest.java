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
import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.field.DataPersister;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.table.TableInfo;

public class HsqldbDatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	private final static String GENERATED_ID_SEQ = "genId_seq";

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:hsqldb:ormlite";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new HsqldbDatabaseType();
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

	@Test(expected = IllegalStateException.class)
	public void testBadGeneratedId() throws Exception {
		Field field = GeneratedId.class.getField("id");
		DatabaseType mockDb = createMock(DatabaseType.class);
		expect(mockDb.isIdSequenceNeeded()).andReturn(false);
		expect(mockDb.getFieldConverter(isA(DataPersister.class))).andReturn(null);
		expect(mockDb.isEntityNamesMustBeUpCase()).andReturn(true);
		replay(mockDb);
		connectionSource.setDatabaseType(mockDb);
		try {
			FieldType fieldType = FieldType.createFieldType(connectionSource, "foo", field, GeneratedId.class);
			verify(mockDb);
			StringBuilder sb = new StringBuilder();
			List<String> statementsBefore = new ArrayList<String>();
			databaseType.appendColumnArg(null, sb, fieldType, null, statementsBefore, null, null);
		} finally {
			connectionSource.setDatabaseType(databaseType);
		}
	}

	@Test
	public void testDropSequence() throws Exception {
		Field field = GeneratedId.class.getField("id");
		FieldType fieldType = FieldType.createFieldType(connectionSource, "foo", field, GeneratedId.class);
		List<String> statementsBefore = new ArrayList<String>();
		List<String> statementsAfter = new ArrayList<String>();
		databaseType.dropColumnArg(fieldType, statementsBefore, statementsAfter);
		assertEquals(0, statementsBefore.size());
		assertEquals(1, statementsAfter.size());
		assertTrue(statementsAfter.get(0).contains("DROP SEQUENCE "));
	}

	@Override
	@Test
	public void testGeneratedIdSequence() throws Exception {
		TableInfo<GeneratedIdSequence, Integer> tableInfo =
				new TableInfo<GeneratedIdSequence, Integer>(connectionSource, null, GeneratedIdSequence.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null,
				null);
		databaseType.addPrimaryKeySql(tableInfo.getFieldTypes(), additionalArgs, statementsBefore, null, null);
		assertTrue(sb + " should contain autoincrement stuff",
				sb.toString().contains(" GENERATED BY DEFAULT AS IDENTITY "));
		// sequence, sequence table, insert
		assertEquals(1, statementsBefore.size());
		assertTrue(statementsBefore.get(0).contains(GENERATED_ID_SEQ.toUpperCase()));
		assertEquals(1, additionalArgs.size());
		assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
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
		databaseType.appendColumnArg(null, sb, idField, additionalArgs, statementsBefore, null, null);
		databaseType.addPrimaryKeySql(new FieldType[] { idField }, additionalArgs, statementsBefore, null, null);
		String seqName =
				databaseType.generateIdSequenceName(GeneratedIdSequenceAutoName.class.getSimpleName().toLowerCase(),
						idField);
		assertTrue(sb + " should contain gen-id-seq-name stuff",
				sb.toString().contains(" GENERATED BY DEFAULT AS IDENTITY "));
		// sequence, sequence table, insert
		assertEquals(1, statementsBefore.size());
		assertTrue(statementsBefore.get(0).contains(seqName.toUpperCase()));
		assertEquals(1, additionalArgs.size());
		assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
	}

	@Override
	@Test
	public void testFieldWidthSupport() {
		assertFalse(databaseType.isVarcharFieldWidthSupported());
	}

	@Override
	@Test
	public void testLimitAfterSelect() {
		assertTrue(databaseType.isLimitAfterSelect());
	}

	@Override
	@Test
	public void testLimitFormat() throws Exception {
		connectionSource.setDatabaseType(databaseType);
		BaseDaoImpl<StringId, String> dao = new BaseDaoImpl<StringId, String>(connectionSource, StringId.class) {
		};
		dao.initialize();
		QueryBuilder<StringId, String> qb = dao.queryBuilder();
		long limit = 1232;
		qb.limit(limit);
		String query = qb.prepareStatementString();
		assertTrue(query + " should start with stuff", query.startsWith("SELECT LIMIT 0 " + limit + " "));
	}

	@Test
	public void testBoolean() throws Exception {
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

	@Override
	protected void testPingValue(long value) {
		assertTrue(value >= 1);
	}

	private final static String LONG_SEQ_NAME = "longseq";

	@Test
	public void testGneratedIdLong() throws Exception {
		TableInfo<GeneratedIdLong, Long> tableInfo =
				new TableInfo<GeneratedIdLong, Long>(connectionSource, null, GeneratedIdLong.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		FieldType idField = tableInfo.getFieldTypes()[0];
		assertEquals("genId", idField.getColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, idField, additionalArgs, statementsBefore, null, null);
		assertEquals(1, statementsBefore.size());
		StringBuilder sb2 = new StringBuilder();
		sb2.append("CREATE SEQUENCE ");
		databaseType.appendEscapedEntityName(sb2, LONG_SEQ_NAME.toUpperCase());
		sb2.append(" AS BIGINT");
		assertTrue(statementsBefore.get(0) + " should contain the right stuff",
				statementsBefore.get(0).contains(sb2.toString()));
	}

	protected static class GeneratedIdLong {
		@DatabaseField(generatedIdSequence = LONG_SEQ_NAME)
		long genId;
		@DatabaseField
		public String stuff;
	}
}
