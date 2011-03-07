package com.j256.ormlite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
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
		TableInfo<GeneratedIdSequence> tableInfo =
				new TableInfo<GeneratedIdSequence>(connectionSource, GeneratedIdSequence.class);
		assertEquals(2, tableInfo.getFieldTypes().length);
		StringBuilder sb = new StringBuilder();
		ArrayList<String> additionalArgs = new ArrayList<String>();
		ArrayList<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null, null);
	}

	@Test
	public void testGeneratedId() throws Exception {
		TableInfo<GeneratedId> tableInfo = new TableInfo<GeneratedId>(connectionSource, GeneratedId.class);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null, null);
		assertTrue(sb + "should contain the stuff", sb.toString().contains(" INTEGER PRIMARY KEY AUTOINCREMENT"));
		assertEquals(0, statementsBefore.size());
		assertEquals(0, additionalArgs.size());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGeneratedIdLong() throws Exception {
		TableInfo<GeneratedIdLong> tableInfo = new TableInfo<GeneratedIdLong>(connectionSource, GeneratedIdLong.class);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null, null);
	}

	@Test
	public void testUsernamePassword() throws Exception {
		closeConnectionSource();
		databaseType = new DerbyEmbeddedDatabaseType();
	}

	@Override
	@Test
	public void testFieldWidthSupport() throws Exception {
		assertFalse(databaseType.isVarcharFieldWidthSupported());
	}

	@Test
	public void testCreateTableReturnsZero() throws Exception {
		assertFalse(databaseType.isCreateTableReturnsZero());
	}

	@Test
	public void testSerialField() throws Exception {
		TableInfo<SerialField> tableInfo = new TableInfo<SerialField>(connectionSource, SerialField.class);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("BLOB"));
	}

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
