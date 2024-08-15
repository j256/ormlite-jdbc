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
import java.sql.Timestamp;

import org.junit.Test;

import com.j256.ormlite.BaseCoreTest;

public class JdbcDatabaseResultsTest extends BaseCoreTest {

	@Test
	public void testGetBlobStream() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		Blob blob = createMock(Blob.class);
		InputStream is = new ByteArrayInputStream(new byte[] {});
		expect(blob.getBinaryStream()).andReturn(is);
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getBlob(1)).andReturn(blob);
		resultSet.close();
		replay(preparedStatement, blob, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertTrue(results.getBlobStream(0) == is);
		results.close();
		verify(preparedStatement, blob, resultSet);
	}

	@Test
	public void testGetBlobStreamNull() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getBlob(1)).andReturn(null);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertNull(results.getBlobStream(0));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testFindColumn() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 21323;
		String name = "name";
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.findColumn(name)).andReturn(colN);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(colN - 1, results.findColumn(name));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetColumnCount() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 21143;
		ResultSetMetaData metaData = createMock(ResultSetMetaData.class);
		expect(resultSet.getMetaData()).andReturn(metaData);
		expect(metaData.getColumnCount()).andReturn(colN);
		resultSet.close();
		replay(preparedStatement, resultSet, metaData);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(colN, results.getColumnCount());
		results.close();
		verify(preparedStatement, resultSet, metaData);
	}

	@Test
	public void testIsNull() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 213;
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.wasNull()).andReturn(true);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertTrue(results.wasNull(colN));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testNext() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.next()).andReturn(true);
		expect(resultSet.next()).andReturn(false);
		expect(preparedStatement.getMoreResults()).andReturn(true);
		expect(resultSet.next()).andReturn(true);
		expect(resultSet.next()).andReturn(false);
		expect(preparedStatement.getMoreResults()).andReturn(false);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertTrue(results.next());
		assertTrue(results.next());
		assertFalse(results.next());
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetBoolean() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		boolean val = true;
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getBoolean(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(val, results.getBoolean(colN));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetByte() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		byte val = 69;
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getByte(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(val, results.getByte(colN));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetBytes() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		byte[] val = new byte[] { 23, 1, 17 };
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getBytes(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertTrue(results.getBytes(colN).equals(val));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetDouble() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		double val = 69.123;
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getDouble(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(val, results.getDouble(colN), 0.0F);
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetFloat() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		float val = 69.77F;
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getFloat(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(val, results.getFloat(colN), 0.0F);
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetInt() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		int val = 613123129;
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getInt(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(val, results.getInt(colN));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetShort() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		short val = 6129;
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getShort(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(val, results.getShort(colN));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetString() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		String val = "zippy";
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getString(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(val, results.getString(colN));
		results.close();
		verify(preparedStatement, resultSet);
	}

	@Test
	public void testGetTimestamp() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		int colN = 120;
		Timestamp val = new Timestamp(123123123123L);
		expect(resultSet.getMetaData()).andReturn(null);
		expect(resultSet.getTimestamp(colN + 1)).andReturn(val);
		resultSet.close();
		replay(preparedStatement, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet, null, false);
		assertEquals(val, results.getTimestamp(colN));
		results.close();
		verify(preparedStatement, resultSet);
	}
}
