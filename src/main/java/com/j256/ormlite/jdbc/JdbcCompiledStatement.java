package com.j256.ormlite.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.j256.ormlite.dao.ObjectCache;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.misc.IOUtils;
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
	private final String statement;
	private final StatementType type;
	private final boolean cacheStore;
	private ResultSetMetaData metaData = null;

	public JdbcCompiledStatement(PreparedStatement preparedStatement, String statement, StatementType type,
			boolean cacheStore) {
		this.preparedStatement = preparedStatement;
		this.statement = statement;
		this.type = type;
		this.cacheStore = cacheStore;
	}

	@Override
	public int getColumnCount() throws SQLException {
		if (metaData == null) {
			metaData = preparedStatement.getMetaData();
		}
		return metaData.getColumnCount();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		if (metaData == null) {
			metaData = preparedStatement.getMetaData();
		}
		return metaData.getColumnName(column + 1);
	}

	@Override
	public int runUpdate() throws SQLException {
		// this can be a UPDATE, DELETE, or ... just not a SELECT
		if (!type.isOkForUpdate()) {
			throw new IllegalArgumentException("Cannot call update on a " + type + " statement");
		}
		return preparedStatement.executeUpdate();
	}

	@Override
	public DatabaseResults runQuery(ObjectCache objectCache) throws SQLException {
		if (!type.isOkForQuery()) {
			throw new IllegalArgumentException("Cannot call query on a " + type + " statement");
		}
		return new JdbcDatabaseResults(preparedStatement, preparedStatement.executeQuery(), objectCache, cacheStore);
	}

	@Override
	public int runExecute() throws SQLException {
		if (!type.isOkForExecute()) {
			throw new IllegalArgumentException("Cannot call execute on a " + type + " statement");
		}
		preparedStatement.execute();
		return preparedStatement.getUpdateCount();
	}

	@Override
	public void close() throws SQLException {
		try {
			preparedStatement.close();
		} catch (SQLException e) {
			throw new SQLException("could not close prepared statement", e);
		}
	}

	@Override
	public void closeQuietly() {
		IOUtils.closeQuietly(this);
	}

	@Override
	public void cancel() throws SQLException {
		preparedStatement.cancel();
	}

	@Override
	public void setObject(int parameterIndex, Object obj, SqlType sqlType) throws SQLException {
		if (obj == null) {
			preparedStatement.setNull(parameterIndex + 1, TypeValMapper.getTypeValForSqlType(sqlType));
		} else {
			preparedStatement.setObject(parameterIndex + 1, obj, TypeValMapper.getTypeValForSqlType(sqlType));
		}
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		preparedStatement.setMaxRows(max);
	}

	@Override
	public void setQueryTimeout(long millis) throws SQLException {
		preparedStatement.setQueryTimeout(Long.valueOf(millis).intValue() / 1000);
	}

	@Override
	public String getStatement() {
		return statement;
	}

	@Override
	public String toString() {
		return statement;
	}

	/**
	 * Called by {@link JdbcDatabaseResults#next()} to get more results into the existing ResultSet.
	 */
	boolean getMoreResults() throws SQLException {
		return preparedStatement.getMoreResults();
	}
}
