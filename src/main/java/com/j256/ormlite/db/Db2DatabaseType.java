package com.j256.ormlite.db;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import com.j256.ormlite.field.*;
import com.j256.ormlite.field.types.OffsetDateTimeType;
import com.j256.ormlite.support.DatabaseResults;

/**
 * IBM DB2 database type information used to create the tables, etc..
 * 
 * <p>
 * <b>WARNING:</b> I have not tested this unfortunately because of a lack of access to a DB2 instance. Love to get 1-2
 * hours of access to an database to test/tweak this. Undoubtably is it wrong. Please contact us if you'd like to help
 * with this class.
 * </p>
 * 
 * @author graywatson
 */
public class Db2DatabaseType extends BaseDatabaseType {

	private final static String DATABASE_URL_PORTION = "db2";
	private final static String DATABASE_NAME = "DB2";
	private final static String DRIVER_CLASS_NAME = "COM.ibm.db2.jdbc.app.DB2Driver";

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
		sb.append("SMALLINT");
	}

	@Override
	protected void appendByteType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("SMALLINT");
	}

	@Override
	protected void appendByteArrayType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("VARCHAR [] FOR BIT DATA");
	}

	@Override
	protected void appendSerializableType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("VARCHAR [] FOR BIT DATA");
	}

	@Override
	protected void configureGeneratedId(String tableName, StringBuilder sb, FieldType fieldType,
			List<String> statementsBefore, List<String> statementsAfter, List<String> additionalArgs,
			List<String> queriesAfter) {
		sb.append("GENERATED ALWAYS AS IDENTITY ");
		configureId(sb, fieldType, statementsBefore, additionalArgs, queriesAfter);
	}

	@Override
	public void appendEscapedEntityName(StringBuilder sb, String name) {
		sb.append('\"').append(name).append('\"');
	}

	@Override
	public boolean isOffsetSqlSupported() {
		// there is no easy way to do this in this database type
		return false;
	}

	@Override
	public FieldConverter getFieldConverter(DataPersister dataType, FieldType fieldType) {
		// we are only overriding certain types
		switch (dataType.getSqlType()) {
			case LOCAL_DATE: // db2 doesn't support JDBC 4.2
				return DataType.LOCAL_DATE_SQL.getDataPersister();
			case LOCAL_TIME:
				return DataType.LOCAL_TIME_SQL.getDataPersister();
			case LOCAL_DATE_TIME:
				return DataType.LOCAL_DATE_TIME_SQL.getDataPersister();
			// db2 doesn't seem to support TIME/STAMP WITH TIME ZONE
			case OFFSET_TIME:
				return null;
			case OFFSET_DATE_TIME:
				return OffsetToLocalDateTimeSqlType.getSingleton();
			default:
				return super.getFieldConverter(dataType, fieldType);
		}
	}

	private static class OffsetToLocalDateTimeSqlType extends OffsetDateTimeType {
		private static final OffsetToLocalDateTimeSqlType singleton = isJavaTimeSupported() ?
				new OffsetToLocalDateTimeSqlType() : null;
		public static OffsetToLocalDateTimeSqlType getSingleton() { return singleton; }
		private OffsetToLocalDateTimeSqlType() { super(SqlType.OFFSET_DATE_TIME, new Class<?>[] { OffsetDateTime.class }); }
		protected OffsetToLocalDateTimeSqlType(SqlType sqlType, Class<?>[] classes) { super(sqlType, classes); }

		@Override
		public Object parseDefaultString(FieldType fieldType, String defaultStr) throws SQLException {
			OffsetDateTime offsetDateTime = (OffsetDateTime) super.parseDefaultString(fieldType, defaultStr);
			// convert to local timezone
			LocalDateTime localDateTime = offsetDateTime.atZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime();
			return Timestamp.valueOf(localDateTime);
		}

		@Override
		public Object resultToSqlArg(FieldType fieldType, DatabaseResults results, int columnPos) throws SQLException {
			return results.getTimestamp(columnPos);
		}

		@Override
		public Object sqlArgToJava(FieldType fieldType, Object sqlArg, int columnPos) {
			Timestamp value = (Timestamp) sqlArg;
			return OffsetDateTime.of(value.toLocalDateTime(), ZoneOffset.of("Z"));
		}

		@Override
		public Object javaToSqlArg(FieldType fieldType, Object javaObject) {
			OffsetDateTime offsetDateTime = (OffsetDateTime) javaObject;
			return Timestamp.valueOf(offsetDateTime.atZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime());
		}
	}
}
