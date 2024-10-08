package com.j256.ormlite.jdbc.db;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import org.junit.jupiter.api.Test;

import com.j256.ormlite.TestUtils;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.FieldConverter;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.support.DatabaseResults;
import com.j256.ormlite.table.TableInfo;

public class DerbyEmbeddedDatabaseTypeTest extends BaseJdbcDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		System.setProperty("derby.stream.error.file", "target/derby.log");
		databaseUrl = "jdbc:derby:target/ormlitederby;create=true";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new DerbyEmbeddedDatabaseType();
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
	public void testGeneratedId() throws Exception {
		TableInfo<GeneratedId, Integer> tableInfo =
				new TableInfo<GeneratedId, Integer>(databaseType, GeneratedId.class);
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, tableInfo.getFieldTypes()[0], additionalArgs, statementsBefore, null,
				null);
		databaseType.addPrimaryKeySql(tableInfo.getFieldTypes(), additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains(" INTEGER GENERATED BY DEFAULT AS IDENTITY"),
				sb + "should contain the stuff");
		assertEquals(0, statementsBefore.size());
		assertEquals(1, additionalArgs.size());
		assertTrue(additionalArgs.get(0).contains("PRIMARY KEY"));
	}

	@Test
	public void testBoolean() throws Exception {
		TableInfo<AllTypes, Integer> tableInfo = new TableInfo<AllTypes, Integer>(databaseType, AllTypes.class);
		assertEquals(9, tableInfo.getFieldTypes().length);
		FieldType booleanField = tableInfo.getFieldTypes()[1];
		assertEquals("booleanField", booleanField.getColumnName());
		StringBuilder sb = new StringBuilder();
		List<String> additionalArgs = new ArrayList<String>();
		List<String> statementsBefore = new ArrayList<String>();
		databaseType.appendColumnArg(null, sb, booleanField, additionalArgs, statementsBefore, null, null);
		assertTrue(sb.toString().contains("SMALLINT"), sb.toString() + " not in right format");
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

	@Override
	@Test
	public void testLimitSupport() {
		assertFalse(databaseType.isLimitSqlSupported());
	}

	@Test
	public void testGetFieldConverterSerializable() {
		DerbyEmbeddedDatabaseType type = new DerbyEmbeddedDatabaseType();
		FieldConverter converter = type.getFieldConverter(DataType.SERIALIZABLE.getDataPersister(), null);
		assertEquals(SqlType.BLOB, converter.getSqlType());
		assertTrue(converter.isStreamType());
	}

	@Test
	public void testObjectFieldConverterParseDefaultString() {
		DerbyEmbeddedDatabaseType type = new DerbyEmbeddedDatabaseType();
		FieldConverter converter = type.getFieldConverter(DataType.SERIALIZABLE.getDataPersister(), null);
		assertThrowsExactly(SQLException.class, () -> {
			converter.parseDefaultString(null, null);
		});
	}

	@Test
	public void testObjectFieldConverterJavaToArgNonSerializable() {
		DerbyEmbeddedDatabaseType type = new DerbyEmbeddedDatabaseType();
		FieldConverter converter = type.getFieldConverter(DataType.SERIALIZABLE.getDataPersister(), null);
		assertThrowsExactly(SQLException.class, () -> {
			converter.javaToSqlArg(null, new NotSerializable());
		});
	}

	@Test
	public void testObjectFieldConverterJavaToArg() throws Exception {
		DerbyEmbeddedDatabaseType type = new DerbyEmbeddedDatabaseType();
		FieldConverter converter = type.getFieldConverter(DataType.SERIALIZABLE.getDataPersister(), null);
		Object object = converter.javaToSqlArg(null, "TEST");
		assertEquals(SerialBlob.class, object.getClass());
	}

	@Test
	public void testObjectFieldConverterResultToJavaStreamNull() throws Exception {
		int COLUMN = 1;
		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getBlobStream(COLUMN)).andReturn(null);
		replay(results);
		DerbyEmbeddedDatabaseType type = new DerbyEmbeddedDatabaseType();
		FieldConverter converter = type.getFieldConverter(DataType.SERIALIZABLE.getDataPersister(), null);
		assertEquals(null, converter.resultToJava(null, results, COLUMN));
		verify(results);
	}

	@Test
	public void testObjectFieldConverterResultToJavaStreamNotObject() throws Exception {
		int COLUMN = 1;
		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		String value = "NotASerializedObject";
		expect(results.getBlobStream(COLUMN)).andReturn(new ByteArrayInputStream(value.getBytes()));
		replay(results);
		DerbyEmbeddedDatabaseType type = new DerbyEmbeddedDatabaseType();
		FieldConverter converter = type.getFieldConverter(DataType.SERIALIZABLE.getDataPersister(), null);
		assertThrowsExactly(SQLException.class, () -> {
			Object obj = converter.resultToJava(null, results, COLUMN);
			assertEquals(value, obj);
		});
		verify(results);
	}

	@Test
	public void testAppendObjectType() {
		StringBuilder sb = new StringBuilder();
		DerbyEmbeddedDatabaseType type = new DerbyEmbeddedDatabaseType();
		type.appendSerializableType(sb, null, 0);
		assertEquals("BLOB", sb.toString());
	}

	@Test
	public void testAppendByteArrayType() {
		StringBuilder sb = new StringBuilder();
		DerbyEmbeddedDatabaseType type = new DerbyEmbeddedDatabaseType();
		type.appendByteArrayType(sb, null, 0);
		assertEquals("LONG VARCHAR FOR BIT DATA", sb.toString());
	}

	private static class NotSerializable {
	}
}
