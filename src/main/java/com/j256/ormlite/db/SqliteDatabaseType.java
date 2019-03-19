package com.j256.ormlite.db;

import com.j256.ormlite.field.DataPersister;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.FieldConverter;
import com.j256.ormlite.field.FieldType;

/**
 * Sqlite database type information used to create the tables, etc..
 * 
 * @author graywatson
 */
public class SqliteDatabaseType extends BaseSqliteDatabaseType {

	private final static String DATABASE_URL_PORTION = "sqlite";
	private final static String DRIVER_CLASS_NAME = "org.sqlite.JDBC";
	private final static String DATABASE_NAME = "SQLite";

	public SqliteDatabaseType() {
	}

	@Override
	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		return DATABASE_URL_PORTION.equals(dbTypePart);
	}

	@Override
	protected String[] getDriverClassNames() {
		return new String[] { DRIVER_CLASS_NAME };
	}

	@Override
	public String getDatabaseName() {
		return DATABASE_NAME;
	}

	@Override
	public void appendLimitValue(StringBuilder sb, long limit, Long offset) {
		sb.append("LIMIT ");
		if (offset != null) {
			sb.append(offset).append(',');
		}
		sb.append(limit).append(' ');
	}

	@Override
	public boolean isOffsetLimitArgument() {
		return true;
	}

	@Override
	public boolean isNestedSavePointsSupported() {
		return false;
	}

	@Override
	public void appendOffsetValue(StringBuilder sb, long offset) {
		throw new IllegalStateException("Offset is part of the LIMIT in database type " + getClass());
	}

	@Override
	public FieldConverter getFieldConverter(DataPersister dataType, FieldType fieldType) {
		// we are only overriding certain types
		switch (dataType.getSqlType()) {
			case LOCAL_DATE: // sqlite doesn't support JDBC 4.2
				return DataType.LOCAL_DATE_SQL.getDataPersister();
			case LOCAL_TIME:
				return DataType.LOCAL_TIME_SQL.getDataPersister();
			case LOCAL_DATE_TIME:
				return DataType.LOCAL_DATE_TIME_SQL.getDataPersister();
			case OFFSET_TIME: // sqlite doesn't seem to support TIME/STAMP WITH TIME ZONE
			case OFFSET_DATE_TIME:
				return null;
			default:
				return super.getFieldConverter(dataType, fieldType);
		}
	}
}
