package com.j256.ormlite.jdbc;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.j256.ormlite.support.DatabaseResults;

/**
 * Wrapper around a {@link ResultSet} object which we delegate to.
 * 
 * @author graywatson
 */
public class JdbcDatabaseResults implements DatabaseResults {

	private final PreparedStatement preparedStmt;
	private final ResultSet resultSet;
	private ResultSetMetaData metaData = null;

	public JdbcDatabaseResults(PreparedStatement preparedStmt, ResultSet resultSet) {
		this.preparedStmt = preparedStmt;
		this.resultSet = resultSet;
	}

	public int getColumnCount() throws SQLException {
		if (metaData == null) {
			metaData = resultSet.getMetaData();
		}
		return metaData.getColumnCount();
	}

	public int findColumn(String columnName) throws SQLException {
		return resultSet.findColumn(columnName) - 1;
	}

	public InputStream getBlobStream(int columnIndex) throws SQLException {
		Blob blob = resultSet.getBlob(columnIndex + 1);
		if (blob == null) {
			return null;
		} else {
			return blob.getBinaryStream();
		}
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		return resultSet.getBoolean(columnIndex + 1);
	}

	public char getChar(int columnIndex) throws SQLException {
		String string = resultSet.getString(columnIndex + 1);
		if (string == null || string.length() == 0) {
			return 0;
		} else if (string.length() == 1) {
			return string.charAt(0);
		} else {
			throw new SQLException("More than 1 character stored in database column: " + columnIndex);
		}
	}

	public byte getByte(int columnIndex) throws SQLException {
		return resultSet.getByte(columnIndex + 1);
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		return resultSet.getBytes(columnIndex + 1);
	}

	public double getDouble(int columnIndex) throws SQLException {
		return resultSet.getDouble(columnIndex + 1);
	}

	public float getFloat(int columnIndex) throws SQLException {
		return resultSet.getFloat(columnIndex + 1);
	}

	public int getInt(int columnIndex) throws SQLException {
		return resultSet.getInt(columnIndex + 1);
	}

	public long getLong(int columnIndex) throws SQLException {
		return resultSet.getLong(columnIndex + 1);
	}

	public short getShort(int columnIndex) throws SQLException {
		return resultSet.getShort(columnIndex + 1);
	}

	public String getString(int columnIndex) throws SQLException {
		return resultSet.getString(columnIndex + 1);
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return resultSet.getTimestamp(columnIndex + 1);
	}

	public boolean next() throws SQLException {
		// NOTE: we should not auto-close here, even if there are no more results
		if (resultSet.next()) {
			return true;
		} else if (!preparedStmt.getMoreResults()) {
			return false;
		} else {
			return resultSet.next();
		}
	}

	public boolean wasNull(int columnIndex) throws SQLException {
		return resultSet.wasNull();
	}
}
