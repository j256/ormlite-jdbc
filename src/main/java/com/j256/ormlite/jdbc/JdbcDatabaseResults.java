package com.j256.ormlite.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.j256.ormlite.dao.ObjectCache;
import com.j256.ormlite.misc.IOUtils;
import com.j256.ormlite.support.DatabaseResults;

/**
 * Wrapper around a {@link ResultSet} object which we delegate to.
 * 
 * @author graywatson
 */
public class JdbcDatabaseResults implements DatabaseResults {

	private final PreparedStatement preparedStmt;
	private final ResultSet resultSet;
	private final ResultSetMetaData metaData;
	private final ObjectCache objectCache;
	private final boolean cacheStore;
	private boolean first = true;

	public JdbcDatabaseResults(PreparedStatement preparedStmt, ResultSet resultSet, ObjectCache objectCache,
			boolean cacheStore) throws SQLException {
		this.preparedStmt = preparedStmt;
		this.resultSet = resultSet;
		this.metaData = resultSet.getMetaData();
		this.objectCache = objectCache;
		this.cacheStore = cacheStore;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return metaData.getColumnCount();
	}

	@Override
	public String[] getColumnNames() throws SQLException {
		int colN = metaData.getColumnCount();
		String[] columnNames = new String[colN];
		for (int colC = 0; colC < colN; colC++) {
			columnNames[colC] = metaData.getColumnLabel(colC + 1);
		}
		return columnNames;
	}

	@Override
	public boolean first() throws SQLException {
		if (first) {
			/*
			 * We have to do this because some databases do not like us calling first() if we are only moving forward
			 * through the results. We do this here because Android has no such issues.
			 */
			first = false;
			return next();
		} else {
			return resultSet.first();
		}
	}

	@Override
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

	@Override
	public boolean last() throws SQLException {
		return resultSet.last();
	}

	@Override
	public boolean previous() throws SQLException {
		return resultSet.previous();
	}

	@Override
	public boolean moveRelative(int offset) throws SQLException {
		return resultSet.relative(offset);
	}

	@Override
	public boolean moveAbsolute(int position) throws SQLException {
		return resultSet.absolute(position);
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		return resultSet.findColumn(columnName) - 1;
	}

	@Override
	public InputStream getBlobStream(int columnIndex) throws SQLException {
		Blob blob = resultSet.getBlob(columnIndex + 1);
		if (blob == null) {
			return null;
		} else {
			return blob.getBinaryStream();
		}
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return resultSet.getBoolean(columnIndex + 1);
	}

	@Override
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

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return resultSet.getByte(columnIndex + 1);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		return resultSet.getBytes(columnIndex + 1);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return resultSet.getDouble(columnIndex + 1);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return resultSet.getFloat(columnIndex + 1);
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return resultSet.getInt(columnIndex + 1);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return resultSet.getLong(columnIndex + 1);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return resultSet.getShort(columnIndex + 1);
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return resultSet.getString(columnIndex + 1);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return resultSet.getTimestamp(columnIndex + 1);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return resultSet.getBigDecimal(columnIndex + 1);
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return resultSet.getObject(columnIndex + 1);
	}

	@Override
	public boolean wasNull(int columnIndex) throws SQLException {
		return resultSet.wasNull();
	}

	@Override
	public ObjectCache getObjectCacheForRetrieve() {
		return objectCache;
	}

	@Override
	public ObjectCache getObjectCacheForStore() {
		if (cacheStore) {
			return objectCache;
		} else {
			return null;
		}
	}

	@Override
	public void close() throws IOException {
		try {
			resultSet.close();
		} catch (SQLException e) {
			throw new IOException("could not close result set", e);
		}
	}

	@Override
	public void closeQuietly() {
		IOUtils.closeQuietly(this);
	}

	/**
	 * Returns the underlying JDBC ResultSet object.
	 */
	public ResultSet getResultSet() {
		return resultSet;
	}
}
