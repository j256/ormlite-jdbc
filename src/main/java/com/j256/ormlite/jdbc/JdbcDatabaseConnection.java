package com.j256.ormlite.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;

import com.j256.ormlite.dao.ObjectCache;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.stmt.GenericRowMapper;
import com.j256.ormlite.stmt.StatementBuilder.StatementType;
import com.j256.ormlite.support.CompiledStatement;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.support.DatabaseResults;
import com.j256.ormlite.support.GeneratedKeyHolder;

/**
 * Wrapper around a JDBC {@link Connection} object which we delegate to.
 * 
 * @author graywatson
 */
public class JdbcDatabaseConnection implements DatabaseConnection {

	private static final String JDBC_META_TABLE_NAME_COLUMN = "TABLE_NAME";

	private static Object[] noArgs = new Object[0];
	private static FieldType[] noArgTypes = new FieldType[0];
	private static GenericRowMapper<Long> longWrapper = new OneLongWrapper();

	private Connection connection;
	private Boolean supportsSavePoints = null;

	public JdbcDatabaseConnection(Connection connection) {
		this.connection = connection;
	}

	public boolean isAutoCommitSupported() throws SQLException {
		return true;
	}

	public boolean getAutoCommit() throws SQLException {
		return connection.getAutoCommit();
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		connection.setAutoCommit(autoCommit);
	}

	public Savepoint setSavePoint(String name) throws SQLException {
		if (supportsSavePoints == null) {
			DatabaseMetaData metaData = connection.getMetaData();
			supportsSavePoints = metaData.supportsSavepoints();
		}
		if (supportsSavePoints) {
			return connection.setSavepoint(name);
		} else {
			return null;
		}
	}

	public void commit(Savepoint savepoint) throws SQLException {
		if (savepoint == null) {
			connection.commit();
		} else {
			connection.releaseSavepoint(savepoint);
		}
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		if (savepoint == null) {
			connection.rollback();
		} else {
			connection.rollback(savepoint);
		}
	}

	public CompiledStatement compileStatement(String statement, StatementType type, FieldType[] argFieldTypes)
			throws SQLException {
		return new JdbcCompiledStatement(connection.prepareStatement(statement, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY), type);
	}

	public void close() throws SQLException {
		connection.close();
	}

	/**
	 * Returns whether the connection has already been closed. Used by {@link JdbcConnectionSource}.
	 */
	public boolean isClosed() throws SQLException {
		return connection.isClosed();
	}

	public int insert(String statement, Object[] args, FieldType[] argFieldTypes, GeneratedKeyHolder keyHolder)
			throws SQLException {
		PreparedStatement stmt;
		if (keyHolder == null) {
			stmt = connection.prepareStatement(statement);
		} else {
			stmt = connection.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
		}
		try {
			statementSetArgs(stmt, args, argFieldTypes);
			int rowN = stmt.executeUpdate();
			if (keyHolder != null) {
				ResultSet resultSet = stmt.getGeneratedKeys();
				ResultSetMetaData metaData = resultSet.getMetaData();
				int colN = metaData.getColumnCount();
				while (resultSet.next()) {
					for (int colC = 1; colC <= colN; colC++) {
						// get the id column data so we can pass it back to the caller thru the keyHolder
						Number id = getIdColumnData(resultSet, metaData, colC);
						keyHolder.addKey(id);
					}
				}
			}
			return rowN;
		} finally {
			stmt.close();
		}
	}

	public int update(String statement, Object[] args, FieldType[] argFieldTypes) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement(statement);
		try {
			statementSetArgs(stmt, args, argFieldTypes);
			return stmt.executeUpdate();
		} finally {
			stmt.close();
		}
	}

	public int delete(String statement, Object[] args, FieldType[] argFieldTypes) throws SQLException {
		// it's a call to executeUpdate
		return update(statement, args, argFieldTypes);
	}

	public <T> Object queryForOne(String statement, Object[] args, FieldType[] argFieldTypes,
			GenericRowMapper<T> rowMapper, ObjectCache objectCache) throws SQLException {
		PreparedStatement stmt =
				connection.prepareStatement(statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		try {
			statementSetArgs(stmt, args, argFieldTypes);
			DatabaseResults results = new JdbcDatabaseResults(stmt, stmt.executeQuery(), objectCache);
			if (!results.next()) {
				// no results at all
				return null;
			}
			T first = rowMapper.mapRow(results);
			if (results.next()) {
				return MORE_THAN_ONE;
			} else {
				return first;
			}
		} finally {
			stmt.close();
		}
	}

	public long queryForLong(String statement) throws SQLException {
		// don't care about the object cache here
		Object result = queryForOne(statement, noArgs, noArgTypes, longWrapper, null);
		if (result == null) {
			throw new SQLException("No results returned in query-for-long: " + statement);
		} else if (result == MORE_THAN_ONE) {
			throw new SQLException("More than 1 result returned in query-for-long: " + statement);
		} else {
			return (Long) result;
		}
	}

	public boolean isTableExists(String tableName) throws SQLException {
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet results = null;
		try {
			results = metaData.getTables(null, null, "%", new String[] { "TABLE" });
			// we do it this way because some result sets don't like us to findColumn if no results
			if (!results.next()) {
				return false;
			}
			int col = results.findColumn(JDBC_META_TABLE_NAME_COLUMN);
			do {
				String dbTableName = results.getString(col);
				if (tableName.equalsIgnoreCase(dbTableName)) {
					return true;
				}
			} while (results.next());
			return false;
		} finally {
			if (results != null) {
				results.close();
			}
		}
	}

	/**
	 * Return the internal database connection. Most likely for testing purposes.
	 */
	public Connection getInternalConnection() {
		return connection;
	}

	/**
	 * Set the internal database connection. Most likely for testing purposes.
	 */
	public void setInternalConnection(Connection connection) {
		this.connection = connection;
	}

	/**
	 * Return the id associated with the column.
	 */
	private Number getIdColumnData(ResultSet resultSet, ResultSetMetaData metaData, int columnIndex)
			throws SQLException {
		int typeVal = metaData.getColumnType(columnIndex);
		switch (typeVal) {
			case Types.BIGINT :
			case Types.DECIMAL :
			case Types.NUMERIC :
				return (Number) resultSet.getLong(columnIndex);
			case Types.INTEGER :
				return (Number) resultSet.getInt(columnIndex);
			default :
				String columnName = metaData.getColumnName(columnIndex);
				throw new SQLException("Unexpected ID column type " + TypeValMapper.getSqlTypeForTypeVal(typeVal)
						+ " (typeVal " + typeVal + ") in column " + columnName + "(#" + columnIndex
						+ ") is not a number");
		}
	}

	private void statementSetArgs(PreparedStatement stmt, Object[] args, FieldType[] argFieldTypes) throws SQLException {
		for (int i = 0; i < args.length; i++) {
			Object arg = args[i];
			int typeVal = TypeValMapper.getTypeValForSqlType(argFieldTypes[i].getSqlType());
			if (arg == null) {
				stmt.setNull(i + 1, typeVal);
			} else {
				stmt.setObject(i + 1, arg, typeVal);
			}
		}
	}

	/**
	 * Row mapper that handles a single long result.
	 */
	private static class OneLongWrapper implements GenericRowMapper<Long> {
		public Long mapRow(DatabaseResults rs) throws SQLException {
			// maps the first column (sql #1)
			return rs.getLong(0);
		}
	}
}
