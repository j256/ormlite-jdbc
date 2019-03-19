package com.j256.ormlite.db;

import java.util.List;

import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DataPersister;
import com.j256.ormlite.field.FieldConverter;

/**
 * H2 database type information used to create the tables, etc..
 * 
 * @author graywatson
 */
public class H2DatabaseType extends BaseDatabaseType {

	private final static String DATABASE_URL_PORTION = "h2";
	private final static String DRIVER_CLASS_NAME = "org.h2.Driver";
	private final static String DATABASE_NAME = "H2";

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
	protected void appendBooleanType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("TINYINT(1)");
	}

	@Override
	protected void configureGeneratedId(String tableName, StringBuilder sb, FieldType fieldType,
			List<String> statementsBefore, List<String> statementsAfter, List<String> additionalArgs,
			List<String> queriesAfter) {
		sb.append("AUTO_INCREMENT ");
		configureId(sb, fieldType, statementsBefore, additionalArgs, queriesAfter);
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
	public void appendOffsetTimeType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("TIMESTAMP WITH TIME ZONE");
	}

	@Override
	public FieldConverter getFieldConverter(DataPersister dataPersister, FieldType fieldType) {
		// H2 doesn't support TIME WITH TIME ZONE
		if (dataPersister.getSqlType() == SqlType.OFFSET_TIME)
			return DataType.OFFSET_TIME_COMPAT.getDataPersister();
		// default is to use the dataPersister itself
		return dataPersister;
	}

	@Override
	public boolean isOffsetLimitArgument() {
		return true;
	}

	@Override
	public void appendOffsetValue(StringBuilder sb, long offset) {
		throw new IllegalStateException("Offset is part of the LIMIT in database type " + getClass());
	}

	@Override
	public boolean isTruncateSupported() {
		return true;
	}

	@Override
	public boolean isCreateIfNotExistsSupported() {
		return true;
	}
}
