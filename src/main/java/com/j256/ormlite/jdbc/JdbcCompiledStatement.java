package com.j256.ormlite.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.stmt.StatementBuilder.StatementType;
import com.j256.ormlite.support.CompiledStatement;
import com.j256.ormlite.support.DatabaseResults;

/**
 * Wrapper around a {@link PreparedStatement} object which we delegate to.
 * 
 * @author graywatson
 */
public class JdbcCompiledStatement implements CompiledStatement {

	private final PreparedStatement preparedStatement;
	private final StatementType type;
	private ResultSetMetaData metaData = null;

	public JdbcCompiledStatement(PreparedStatement preparedStatement, StatementType type) {
		this.preparedStatement = preparedStatement;
		this.type = type;
	}

	public int getColumnCount() throws SQLException {
		if (metaData == null) {
			metaData = preparedStatement.getMetaData();
		}
		return metaData.getColumnCount();
	}

	public String getColumnName(int column) throws SQLException {
		if (metaData == null) {
			metaData = preparedStatement.getMetaData();
		}
		return metaData.getColumnName(column + 1);
	}

	public int runUpdate() throws SQLException {
		// this can be a UPDATE, DELETE, or ... just not a SELECT
		if (type == StatementType.SELECT) {
			throw new IllegalArgumentException("Cannot call update on a " + type + " statement");
		}
		return preparedStatement.executeUpdate();
	}

	public DatabaseResults runQuery() throws SQLException {
		if (type != StatementType.SELECT) {
			throw new IllegalArgumentException("Cannot call query on a " + type + " statement");
		}
		return new JdbcDatabaseResults(preparedStatement, preparedStatement.executeQuery());
	}

	public int runExecute() throws SQLException {
		if (type != StatementType.EXECUTE) {
			throw new IllegalArgumentException("Cannot call execute on a " + type + " statement");
		}
		preparedStatement.execute();
		return preparedStatement.getUpdateCount();
	}

	public DatabaseResults getGeneratedKeys() throws SQLException {
		return new JdbcDatabaseResults(preparedStatement, preparedStatement.getGeneratedKeys());
	}

	public void close() throws SQLException {
		preparedStatement.close();
	}

	public void setNull(int parameterIndex, SqlType sqlType) throws SQLException {
		preparedStatement.setNull(parameterIndex + 1, TypeValMapper.getTypeValForSqlType(sqlType));
	}

	public void setObject(int parameterIndex, Object obj, SqlType sqlType) throws SQLException {
		preparedStatement.setObject(parameterIndex + 1, obj, TypeValMapper.getTypeValForSqlType(sqlType));
	}

	public void setMaxRows(int max) throws SQLException {
		preparedStatement.setMaxRows(max);
	}

	/**
	 * Called by {@link JdbcDatabaseResults#next()} to get more results into the existing ResultSet.
	 */
	boolean getMoreResults() throws SQLException {
		return preparedStatement.getMoreResults();
	}
}
