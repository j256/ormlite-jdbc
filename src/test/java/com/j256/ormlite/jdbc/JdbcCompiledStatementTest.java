package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.easymock.EasyMock;
import org.junit.Test;

import com.j256.ormlite.BaseCoreTest;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.stmt.StatementBuilder.StatementType;

public class JdbcCompiledStatementTest extends BaseCoreTest {

	@Test
	public void testGetColumnName() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSetMetaData metadata = createMock(ResultSetMetaData.class);
		expect(metadata.getColumnName(1)).andReturn("TEST_COLUMN1");
		expect(preparedStatement.getMetaData()).andReturn(metadata);
		replay(metadata, preparedStatement);
		JdbcCompiledStatement stmt = new JdbcCompiledStatement(preparedStatement, StatementType.SELECT);
		assertEquals("TEST_COLUMN1", stmt.getColumnName(0));
		verify(preparedStatement, metadata);
	}

	@Test
	public void testGetMoreResults() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		expect(preparedStatement.getMoreResults()).andReturn(Boolean.TRUE);
		replay(preparedStatement);
		JdbcCompiledStatement stmt = new JdbcCompiledStatement(preparedStatement, StatementType.SELECT);
		stmt.getMoreResults();
		verify(preparedStatement);
	}

	@Test
	public void testSetNull() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		preparedStatement.setNull(1, TypeValMapper.getTypeValForSqlType(SqlType.STRING));
		EasyMock.expectLastCall();
		replay(preparedStatement);
		JdbcCompiledStatement stmt = new JdbcCompiledStatement(preparedStatement, StatementType.SELECT);
		stmt.setObject(0, null, SqlType.STRING);
		verify(preparedStatement);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecuteUpdateWithSelectType() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		JdbcCompiledStatement stmt = new JdbcCompiledStatement(preparedStatement, StatementType.SELECT);
		stmt.runUpdate();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecuteQueryWithNonSelectType() throws SQLException {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		JdbcCompiledStatement stmt = new JdbcCompiledStatement(preparedStatement, StatementType.EXECUTE);
		stmt.runQuery(null);
	}
}
