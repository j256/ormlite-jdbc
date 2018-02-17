package com.j256.ormlite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.j256.ormlite.BaseJdbcTest;
import com.j256.ormlite.TestUtils;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.table.TableInfo;

/**
 * Base test for other database tests which perform specific functionality tests on all databases.
 */
public abstract class BaseJdbcDatabaseTypeTest extends BaseJdbcTest {

	private final static String DATABASE_NAME = "ormlite";
	private final String DB_DIRECTORY = "target/" + getClass().getSimpleName();

	protected final static String GENERATED_ID_SEQ = "genId_seq";

	@Test
	public void testCommentLinePrefix() {
		assertEquals("-- ", databaseType.getCommentLinePrefix());
	}

	@Test
	public void testEscapedEntityName() {
		String word = "word";
		assertEquals("`" + word + "`", TestUtils.appendEscapedEntityName(databaseType, word));
	}

	@Test
	public void testEscapedWord() {
		String word = "word";
		assertEquals("'" + word + "'", TestUtils.appendEscapedWord(databaseType, word));
	}

	@Test
	public void testCreateColumnArg() throws Exception {
		if (connectionSource == null) {
			return;
		}
		List<String> additionalArgs = new ArrayList<String>();
		List<String> moreStmts = new ArrayList<String>();
		List<String> queriesAfter = new ArrayList<String>();
		TableInfo<StringId, String> tableInfo = new TableInfo<StringId, String>(connectionSource, null, StringId.class);
		FieldType fieldType = tableInfo.getIdField();
		StringBuilder sb = new StringBuilder();
		databaseType.appendColumnArg(null, sb, fieldType, additionalArgs, null, moreStmts, queriesAfter);
		assertTrue(sb.toString().contains(fieldType.getColumnName()));
		if (!sb.toString().contains("PRIMARY KEY")) {
			databaseType.addPrimaryKeySql(new FieldType[] { fieldType }, additionalArgs, null, moreStmts, queriesAfter);
			assertEquals(1, additionalArgs.size());
			assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
		}
	}

	@Test
	public void testFileSystem() throws Exception {
		File dbDir = new File(DB_DIRECTORY);
		TestUtils.deleteDirectory(dbDir);
		dbDir.mkdirs();
		assertEquals(0, dbDir.list().length);
		closeConnectionSource();
		String dbUrl = "jdbc:h2:" + dbDir.getPath() + "/" + DATABASE_NAME;
		connectionSource = new JdbcConnectionSource(dbUrl);
		DatabaseConnection conn = connectionSource.getReadWriteConnection(null);
		try {
			databaseType = DatabaseTypeUtils.createDatabaseType(dbUrl);
			assertTrue(dbDir.list().length != 0);
		} finally {
			connectionSource.releaseConnection(conn);
		}
	}

	@Test
	public void testFieldWidthSupport() {
		assertTrue(databaseType.isVarcharFieldWidthSupported());
	}

	@Test
	public void testLimitSupport() {
		assertTrue(databaseType.isLimitSqlSupported());
	}

	@Test
	public void testLimitAfterSelect() {
		assertFalse(databaseType.isLimitAfterSelect());
	}

	@Test
	public void testLimitFormat() throws Exception {
		if (connectionSource == null) {
			return;
		}
		if (!databaseType.isLimitSqlSupported()) {
			return;
		}
		TableInfo<StringId, String> tableInfo = new TableInfo<StringId, String>(connectionSource, null, StringId.class);
		QueryBuilder<StringId, String> qb = new QueryBuilder<StringId, String>(databaseType, tableInfo, null);
		long limit = 1232;
		qb.limit(limit);
		String query = qb.prepareStatementString();
		assertTrue(query + " should contain LIMIT", query.contains(" LIMIT " + limit + " "));
	}

	@Test
	public void testOffsetSupport() {
		assertTrue(databaseType.isOffsetSqlSupported());
	}

	@Test(expected = SQLException.class)
	public void testLoadDriver() throws Exception {
		if (isDriverClassExpected()) {
			throw new SQLException("We have the class so simulate a failure");
		} else {
			databaseType.loadDriver();
		}
	}

	@Test(expected = SQLException.class)
	public void testGeneratedIdSequence() throws Exception {
		if (connectionSource == null) {
			throw new SQLException("Simulate a failure");
		}
		TableInfo<GeneratedIdSequence, Integer> tableInfo =
				new TableInfo<GeneratedIdSequence, Integer>(connectionSource, null, GeneratedIdSequence.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		StringBuilder sb = new StringBuilder();
		ArrayList<String> additionalArgs = new ArrayList<String>();
		ArrayList<String> statementsBefore = new ArrayList<String>();
		ArrayList<String> statementsAfter = new ArrayList<String>();
		List<String> queriesAfter = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore,
				statementsAfter, queriesAfter);
	}

	/**
	 * Return the ping value so we can test a connection.
	 */
	protected void testPingValue(long value) {
		assertEquals(1, value);
	}

	@Test
	public void testDatabasePing() throws Exception {
		if (connectionSource == null) {
			return;
		}
		if (!isDriverClassExpected()) {
			return;
		}
		String ping = databaseType.getPingStatement();
		DatabaseConnection conn = connectionSource.getReadOnlyConnection(null);
		try {
			testPingValue(conn.queryForLong(ping));
		} finally {
			connectionSource.releaseConnection(conn);
		}
	}

	protected static class StringId {
		@DatabaseField(id = true)
		String id;
	}

	protected static class GeneratedId {
		@DatabaseField(generatedId = true)
		public int id;
		@DatabaseField
		String other;

		public GeneratedId() {
		}
	}

	protected static class GeneratedIdSequence {
		@DatabaseField(generatedIdSequence = GENERATED_ID_SEQ)
		public int genId;
		@DatabaseField
		public String stuff;

		protected GeneratedIdSequence() {
		}
	}

	protected static class GeneratedIdSequenceAutoName {
		@DatabaseField(generatedId = true)
		int genId;
		@DatabaseField
		public String stuff;
	}

	protected static class AllTypes {
		@DatabaseField
		String stringField;
		@DatabaseField
		boolean booleanField;
		@DatabaseField
		Date dateField;
		@DatabaseField
		byte byteField;
		@DatabaseField
		short shortField;
		@DatabaseField
		int intField;
		@DatabaseField
		long longField;
		@DatabaseField
		float floatField;
		@DatabaseField
		double doubleField;

		AllTypes() {
		}
	}
}
