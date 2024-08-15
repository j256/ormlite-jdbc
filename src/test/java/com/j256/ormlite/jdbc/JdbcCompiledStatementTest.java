package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;

import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.j256.ormlite.BaseCoreTest;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.stmt.StatementBuilder.StatementType;

public class JdbcCompiledStatementTest extends BaseCoreTest {

	@Test
	public void testGetColumnName() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		ResultSetMetaData metadata = createMock(ResultSetMetaData.class);
		expect(metadata.getColumnName(1)).andReturn("TEST_COLUMN1");
		expect(preparedStatement.getMetaData()).andReturn(metadata);
		preparedStatement.close();
		replay(metadata, preparedStatement);
		String statement = "statement";
		JdbcCompiledStatement stmt =
				new JdbcCompiledStatement(preparedStatement, statement, StatementType.SELECT, false);
		assertEquals(statement, stmt.getStatement());
		assertEquals(statement, stmt.toString());
		assertEquals("TEST_COLUMN1", stmt.getColumnName(0));
		stmt.close();
		verify(preparedStatement, metadata);
	}

	@Test
	public void testGetMoreResults() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		expect(preparedStatement.getMoreResults()).andReturn(Boolean.TRUE);
		preparedStatement.close();
		replay(preparedStatement);
		JdbcCompiledStatement stmt =
				new JdbcCompiledStatement(preparedStatement, "statement", StatementType.SELECT, false);
		stmt.getMoreResults();
		stmt.close();
		verify(preparedStatement);
	}

	@Test
	public void testSetNull() throws Exception {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		preparedStatement.setNull(1, TypeValMapper.getTypeValForSqlType(SqlType.STRING));
		EasyMock.expectLastCall();
		preparedStatement.close();
		replay(preparedStatement);
		JdbcCompiledStatement stmt =
				new JdbcCompiledStatement(preparedStatement, "statement", StatementType.SELECT, false);
		stmt.setObject(0, null, SqlType.STRING);
		stmt.close();
		verify(preparedStatement);
	}

	@Test
	public void testExecuteUpdateWithSelectType() {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		JdbcCompiledStatement stmt =
				new JdbcCompiledStatement(preparedStatement, "statement", StatementType.SELECT, false);
		assertThrowsExactly(IllegalArgumentException.class, () -> {
			stmt.runUpdate();
			stmt.close();
		});
	}

	@Test
	public void testExecuteQueryWithNonSelectType() {
		PreparedStatement preparedStatement = createMock(PreparedStatement.class);
		JdbcCompiledStatement stmt =
				new JdbcCompiledStatement(preparedStatement, "statement", StatementType.EXECUTE, false);
		assertThrowsExactly(IllegalArgumentException.class, () -> {
			stmt.runQuery(null);
			stmt.close();
		});
	}
}
