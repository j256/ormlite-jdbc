package com.j256.ormlite.db;

import java.sql.SQLException;
import java.util.List;

import com.j256.ormlite.field.BaseFieldConverter;
import com.j256.ormlite.field.DataPersister;
import com.j256.ormlite.field.FieldConverter;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.support.DatabaseResults;

/**
 * Microsoft SQL server database type information used to create the tables, etc..
 * 
 * <p>
 * <b>WARNING:</b> I have not tested this unfortunately because of a lack of permanent access to a MSSQL instance.
 * </p>
 * 
 * @author graywatson
 */
public class SqlServerDatabaseType extends BaseDatabaseType {

	private final static String DATABASE_URL_PORTION = "sqlserver";
	private final static String DRIVER_CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private final static String DATABASE_NAME = "SQL Server";

	private final static FieldConverter byteConverter = new ByteFieldConverter();
	private final static FieldConverter booleanConverter = new BooleanNumberFieldConverter();

	@Override
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
	public FieldConverter getFieldConverter(DataPersister dataType, FieldType fieldType) {
		// we are only overriding certain types
		switch (dataType.getSqlType()) {
			case BOOLEAN:
				return booleanConverter;
			case BYTE:
				return byteConverter;
			default:
				return super.getFieldConverter(dataType, fieldType);
		}
	}

	@Override
	protected void appendUuidNativeType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("UNIQUEIDENTIFIER");
	}

	@Override
	protected void appendBooleanType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("BIT");
	}

	@Override
	protected void appendByteType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		// TINYINT exists but it gives 0-255 unsigned
		// http://msdn.microsoft.com/en-us/library/ms187745.aspx
		sb.append("SMALLINT");
	}

	@Override
	protected void appendDateType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		// TIMESTAMP is some sort of internal database type
		// http://www.sqlteam.com/article/timestamps-vs-datetime-data-types
		sb.append("DATETIME");
	}

	@Override
	protected void appendByteArrayType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("IMAGE");
	}

	@Override
	protected void appendSerializableType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("IMAGE");
	}

	@Override
	protected void configureGeneratedId(String tableName, StringBuilder sb, FieldType fieldType,
			List<String> statementsBefore, List<String> statementsAfter, List<String> additionalArgs,
			List<String> queriesAfter) {
		sb.append("IDENTITY ");
		configureId(sb, fieldType, statementsBefore, additionalArgs, queriesAfter);
		if (fieldType.isAllowGeneratedIdInsert()) {
			StringBuilder identityInsertSb = new StringBuilder();
			identityInsertSb.append("SET IDENTITY_INSERT ");
			appendEscapedEntityName(identityInsertSb, tableName);
			identityInsertSb.append(" ON");
			statementsAfter.add(identityInsertSb.toString());
		}
	}

	@Override
	public void appendEscapedEntityName(final StringBuilder sb, final String name) {
		String[] names = name.split("\\.");
		appendEscapedEntityNamePart(sb, names[0]);
		for (int i = 1; i < names.length; i++) {
			sb.append('.');
			appendEscapedEntityNamePart(sb, names[i]);
		}
	}

	private void appendEscapedEntityNamePart(final StringBuilder sb, final String name) {
		sb.append('[').append(name).append(']');
	}

	@Override
	public boolean isLimitAfterSelect() {
		return true;
	}

	@Override
	public void appendLimitValue(StringBuilder sb, long limit, Long offset) {
		sb.append("TOP ").append(limit).append(' ');
	}

	@Override
	public boolean isOffsetSqlSupported() {
		// there is no easy way to do this in this database type
		return false;
	}

	@Override
	public boolean isAllowGeneratedIdInsertSupported() {
		/*
		 * The only way sql-server allows this is if "SET IDENTITY_INSERT table-name ON" has been set. However this is a
		 * runtime session value and not a configuration option. Grrrrr.
		 */
		return false;
	}

	@Override
	public boolean isCreateTableReturnsNegative() {
		return true;
	}

	@Override
	public void appendInsertNoColumns(StringBuilder sb) {
		sb.append("DEFAULT VALUES");
	}

	@Override
	public boolean isTruncateSupported() {
		return true;
	}

	/**
	 * Conversion from the byte Java field to the SMALLINT Jdbc type because TINYINT looks to be 0-255 and unsigned.
	 */
	private static class ByteFieldConverter extends BaseFieldConverter {
		@Override
		public SqlType getSqlType() {
			// store it as a short
			return SqlType.BYTE;
		}

		@Override
		public Object parseDefaultString(FieldType fieldType, String defaultStr) {
			return Short.parseShort(defaultStr);
		}

		@Override
		public Object resultToSqlArg(FieldType fieldType, DatabaseResults results, int columnPos) throws SQLException {
			// starts as a short and then gets converted to a byte on the way out
			return results.getShort(columnPos);
		}

		@Override
		public Object sqlArgToJava(FieldType fieldType, Object sqlObject, int columnPos) {
			short shortVal = (Short) sqlObject;
			// make sure the database value doesn't overflow the byte
			if (shortVal < Byte.MIN_VALUE) {
				return Byte.MIN_VALUE;
			} else if (shortVal > Byte.MAX_VALUE) {
				return Byte.MAX_VALUE;
			} else {
				return (byte) shortVal;
			}
		}

		@Override
		public Object javaToSqlArg(FieldType fieldType, Object javaObject) {
			// convert the Byte arg to be a short
			byte byteVal = (Byte) javaObject;
			return (short) byteVal;
		}

		@Override
		public Object resultStringToJava(FieldType fieldType, String stringValue, int columnPos) {
			return sqlArgToJava(fieldType, Short.parseShort(stringValue), columnPos);
		}
	}
}
