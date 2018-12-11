package com.j256.ormlite.db;

import java.util.List;

import com.j256.ormlite.field.DataPersister;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.FieldConverter;
import com.j256.ormlite.field.FieldType;

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
	protected String getDriverClassName() {
		return DRIVER_CLASS_NAME;
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
			case OFFSET_TIME: // db2 doesn't seem to support TIME/STAMP WITH TIME ZONE
			case OFFSET_DATE_TIME:
				return null;
			default:
				return super.getFieldConverter(dataType, fieldType);
		}
	}
}
