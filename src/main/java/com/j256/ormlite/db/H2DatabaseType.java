package com.j256.ormlite.db;

import java.util.List;

import com.j256.ormlite.field.FieldType;

/**
 * H2 database type information used to create the tables, etc..
 * 
 * @author graywatson
 */
public class H2DatabaseType extends BaseDatabaseType implements DatabaseType {

	private final static String DATABASE_URL_PORTION = "h2";
	private final static String DRIVER_CLASS_NAME = "org.h2.Driver";
	private final static String DATABASE_NAME = "H2";

	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		return DATABASE_URL_PORTION.equals(dbTypePart);
	}

	@Override
	protected String getDriverClassName() {
		return DRIVER_CLASS_NAME;
	}

	@Override
	public String getDatabaseName() {
		return DATABASE_NAME;
	}

	@Override
	protected void appendBooleanType(StringBuilder sb) {
		sb.append("TINYINT(1)");
	}

	@Override
	protected void configureGeneratedId(StringBuilder sb, FieldType fieldType, List<String> statementsBefore,
			List<String> additionalArgs, List<String> queriesAfter) {
		sb.append("AUTO_INCREMENT ");
		configureId(sb, fieldType, statementsBefore, additionalArgs, queriesAfter);
	}

	@Override
	public void appendLimitValue(StringBuilder sb, int limit, Integer offset) {
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
	public void appendOffsetValue(StringBuilder sb, int offset) {
		throw new IllegalStateException("Offset is part of the LIMIT in database type " + getClass());
	}

	@Override
	public boolean isTruncateSupported() {
		return true;
	}
}
