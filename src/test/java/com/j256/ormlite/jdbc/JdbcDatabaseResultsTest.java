package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.junit.Test;

import com.j256.ormlite.BaseCoreTest;

public class JdbcDatabaseResultsTest extends BaseCoreTest {

	@Test
	public void testGetColumnName() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		ResultSetMetaData metadata = createMock(ResultSetMetaData.class);
		expect(metadata.getColumnName(1)).andReturn("TEST_COLUMN1");
		expect(resultSet.getMetaData()).andReturn(metadata);
		replay(preparedStatement, metadata, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals("TEST_COLUMN1", results.getColumnName(1));
		verify(preparedStatement, metadata, resultSet);
	}

	@Test
	public void testGetBlobStream() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		Blob blob = createMock(Blob.class);
		InputStream is = new ByteArrayInputStream(new byte[] {});
		expect(blob.getBinaryStream()).andReturn(is);
		expect(resultSet.getBlob(0)).andReturn(blob);
		replay(preparedStatement, blob, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertTrue(results.getBlobStream(0) == is);
		verify(preparedStatement, blob, resultSet);
	}

	@Test
	public void testGetBlobStreamNull() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		expect(resultSet.getBlob(0)).andReturn(null);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertNull(results.getBlobStream(0));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetIdColumnData() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		ResultSetMetaData metaData = createMock(ResultSetMetaData.class);
		expect(resultSet.getMetaData()).andReturn(metaData);
		int colType = Types.BIGINT;
		int colN = 0;
		expect(metaData.getColumnType(colN)).andReturn(colType);
		long val = 123213213L;
		expect(resultSet.getLong(colN)).andReturn(val);
		replay(preparedStatement, resultSet, metaData);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getIdColumnData(colN));
		verify(preparedStatement, resultSet, metaData);
	}

	@Test(expected = SQLException.class)
	public void testGetUnknownIdType() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		ResultSetMetaData metaData = createMock(ResultSetMetaData.class);
		expect(resultSet.getMetaData()).andReturn(metaData);
		int colType = Types.VARCHAR;
		int colN = 0;
		expect(metaData.getColumnType(colN)).andReturn(colType);
		replay(preparedStatement, resultSet, metaData);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		results.getIdColumnData(colN);
	}

	@Test
	public void testFindColumn() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 21323;
		String name = "name";
		expect(resultSet.findColumn(name)).andReturn(colN);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		replay(preparedStatement, resultSet);
		assertEquals(colN, results.findColumn(name));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetColumnCount() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 21143;
		ResultSetMetaData metaData = createMock(ResultSetMetaData.class);
		expect(resultSet.getMetaData()).andReturn(metaData);
		expect(metaData.getColumnCount()).andReturn(colN);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		replay(preparedStatement, resultSet, metaData);
		assertEquals(colN, results.getColumnCount());
		verify(preparedStatement, resultSet, metaData);
	}

	@Test
	public void testIsNull() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 213;
		expect(resultSet.getObject(colN)).andReturn(null);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		replay(preparedStatement, resultSet);
		assertTrue(results.isNull(colN));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testNext() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		expect(resultSet.next()).andReturn(true);
		expect(resultSet.next()).andReturn(false);
		expect(preparedStatement.getMoreResults()).andReturn(true);
		expect(resultSet.next()).andReturn(true);
		expect(resultSet.next()).andReturn(false);
		expect(preparedStatement.getMoreResults()).andReturn(false);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		replay(preparedStatement, resultSet);
		assertTrue(results.next());
		assertTrue(results.next());
		assertFalse(results.next());
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetBoolean() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		boolean val = true;
		expect(resultSet.getBoolean(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getBoolean(colN));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetByte() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		byte val = 69;
		expect(resultSet.getByte(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getByte(colN));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetBytes() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		byte[] val = new byte[] { 23, 1, 17 };
		expect(resultSet.getBytes(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getBytes(colN));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetDouble() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		double val = 69.123;
		expect(resultSet.getDouble(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getDouble(colN), 0.0F);
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetFloat() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		float val = 69.77F;
		expect(resultSet.getFloat(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getFloat(colN), 0.0F);
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetInt() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		int val = 613123129;
		expect(resultSet.getInt(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getInt(colN));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetShort() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		short val = 6129;
		expect(resultSet.getShort(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getShort(colN));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetString() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		String val = "zippy";
		expect(resultSet.getString(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getString(colN));
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetTimestamp() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		Timestamp val = new Timestamp(123123123123L);
		expect(resultSet.getTimestamp(colN)).andReturn(val);
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals(val, results.getTimestamp(colN));
		verify(preparedStatement, resultSet);
	}
}
