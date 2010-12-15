package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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
		replay(metadata, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertEquals("TEST_COLUMN1", results.getColumnName(1));
		verify(metadata, resultSet);
	}

	@Test
	public void testGetBlobStream() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		Blob blob = createMock(Blob.class);
		InputStream is = new ByteArrayInputStream(new byte[] {});
		expect(blob.getBinaryStream()).andReturn(is);
		expect(resultSet.getBlob(0)).andReturn(blob);
		replay(blob, resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertTrue(results.getBlobStream(0) == is);
		verify(blob, resultSet);
	}

	@Test
	public void testGetBlobStreamNull() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSet resultSet = createMock(ResultSet.class);
		expect(resultSet.getBlob(0)).andReturn(null);
		replay(resultSet);
		JdbcDatabaseResults results = new JdbcDatabaseResults(preparedStatement, resultSet);
		assertNull(results.getBlobStream(0));
		verify(resultSet);
	}
}
