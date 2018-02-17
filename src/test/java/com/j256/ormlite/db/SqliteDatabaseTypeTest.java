package com.j256.ormlite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.GenericRawResults;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.table.TableInfo;

public class SqliteDatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:sqlite:";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new SqliteDatabaseType();
	}

	@Override
	protected boolean isDriverClassExpected() {
		return false;
	}

	@Test(expected = SQLException.class)
	public void testGeneratedIdSequenceNotSupported() throws Exception {
		TableInfo<GeneratedIdSequence, Integer> tableInfo =
				new TableInfo<GeneratedIdSequence, Integer>(connectionSource, null, GeneratedIdSequence.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		StringBuilder sb = new StringBuilder();
		ArrayList<String> additionalArgs = new ArrayList<String>();
		ArrayList<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null,
				null);
	}

	@Test
	public void testGeneratedId() throws Exception {
		TableInfo<GeneratedId, Integer> tableInfo =
				new TableInfo<GeneratedId, Integer>(connectionSource, null, GeneratedId.class);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null,
				null);
		databaseType.addPrimaryKeySql(tableInfo.getFieldTypes(), additionalArgs, statementsBefore, null, null);
		assertTrue(sb + "should contain the stuff", sb.toString().contains(" INTEGER PRIMARY KEY AUTOINCREMENT"));
		assertEquals(0, statementsBefore.size());
		assertEquals(0, additionalArgs.size());
	}

	@Override
	@Test
	public void testFieldWidthSupport() {
		assertFalse(databaseType.isVarcharFieldWidthSupported());
	}

	@Test
	public void testCreateTableReturnsZero() {
		assertFalse(databaseType.isCreateTableReturnsZero());
	}

	@Test
	public void testSerialField() throws Exception {
		TableInfo<SerialField, Void> tableInfo =
				new TableInfo<SerialField, Void>(connectionSource, null, SerialField.class);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null,
				null);
		assertTrue(sb.toString().contains("BLOB"));
	}

	@Test
	public void testDateFormat() throws Exception {
		Dao<AllTypes, Object> dao = createDao(AllTypes.class, true);
		AllTypes all = new AllTypes();
		all.dateField = new Date();
		assertEquals(1, dao.create(all));
		GenericRawResults<String[]> results = dao.queryRaw("select * from alltypes");
		List<String[]> stringslist = results.getResults();
		String[] names = results.getColumnNames();
		for (String[] strings : stringslist) {
			for (int i = 0; i < strings.length; i++) {
				System.out.println(names[i] + "=" + strings[i]);
			}
		}
	}

	@Test
	public void testLimitOffsetFormat() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TableInfo<StringId, String> tableInfo = new TableInfo<StringId, String>(connectionSource, null, StringId.class);
		QueryBuilder<StringId, String> qb = new QueryBuilder<StringId, String>(databaseType, tableInfo, null);
		long limit = 1232;
		qb.limit(limit);
		long offset = 171;
		qb.offset(offset);
		String query = qb.prepareStatementString();
		assertTrue(query + " should contain LIMIT", query.contains(" LIMIT " + offset + "," + limit + " "));
	}

	@Test
	public void testIsOffsetLimitArgument() {
		assertTrue(databaseType.isOffsetLimitArgument());
	}

	@Test
	public void testIsNestedSavePointsSupported() {
		assertFalse(databaseType.isNestedSavePointsSupported());
	}

	@Test(expected = IllegalStateException.class)
	public void testfIsNestedSavePointsSupported() {
		databaseType.appendOffsetValue(null, 0);
	}

	/* ==================================================================== */

	protected static class GeneratedIdLong {
		@DatabaseField(generatedId = true)
		public long id;
		@DatabaseField
		String other;

		public GeneratedIdLong() {
		}
	}

	protected static class SerialField {
		@DatabaseField(dataType = DataType.SERIALIZABLE)
		SerializedThing other;

		public SerialField() {
		}
	}

	protected static class SerializedThing implements Serializable {
		private static final long serialVersionUID = -7989929665216767119L;
		@DatabaseField
		String other;

		public SerializedThing() {
		}
	}
}
