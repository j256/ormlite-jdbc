package com.j256.ormlite.db;

import java.util.List;

import com.j256.ormlite.field.DataPersister;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.FieldConverter;
import com.j256.ormlite.field.FieldType;

/**
 * Oracle database type information used to create the tables, etc..
 * 
 * <p>
 * <b>WARNING:</b> I have not tested this unfortunately because of a lack of access to a Oracle instance. Love to get
 * 1-2 hours of access to an database to test/tweak this. Undoubtably is it wrong. Please contact Please contact us if
 * you'd like to help with this class.
 * </p>
 * 
 * @author graywatson
 */
public class OracleDatabaseType extends BaseDatabaseType {

	private final static String DATABASE_URL_PORTION = "oracle";
	private final static String DRIVER_CLASS_NAME = "oracle.jdbc.driver.OracleDriver";
	private final static String DATABASE_NAME = "Oracle";
	private static final String BOOLEAN_INTEGER_FORMAT = "integer";

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
	protected void appendStringType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("VARCHAR2(").append(fieldWidth).append(')');
	}

	@Override
	protected void appendLongStringType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("LONG");
	}

	@Override
	protected void appendByteType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("SMALLINT");
	}

	@Override
	protected void appendLongType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("NUMERIC");
	}

	@Override
	protected void appendByteArrayType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("LONG RAW");
	}

	@Override
	protected void appendSerializableType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("LONG RAW");
	}

	@Override
	protected void appendBigDecimalNumericType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		// from stew
		sb.append("NUMBER(*," + fieldWidth + ")");
	}

	@Override
	protected void appendBooleanType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		if (BOOLEAN_INTEGER_FORMAT.equalsIgnoreCase(fieldType.getFormat())) {
			sb.append("INTEGER");
		} else {
			sb.append("CHAR(1)");
		}
	}

	@Override
	public FieldConverter getFieldConverter(DataPersister dataPersister, FieldType fieldType) {
		switch (dataPersister.getSqlType()) {
			case BOOLEAN:
				/*
				 * Booleans in Oracle are stored as the character '1' or '0'. You can change the characters by
				 * specifying a format string. It must be a string with 2 characters. The first character is the value
				 * for TRUE, the second is FALSE. See {@link BooleanCharType}.
				 * 
				 * You can also specify the format as "integer" to use an integer column type and the value 1 (really
				 * non-0) for true and 0 for false. See {@link BooleanIntegerType}.
				 */
				if (BOOLEAN_INTEGER_FORMAT.equalsIgnoreCase(fieldType.getFormat())) {
					return DataType.BOOLEAN_INTEGER.getDataPersister();
				} else {
					return DataType.BOOLEAN_CHAR.getDataPersister();
				}
			default:
				return super.getFieldConverter(dataPersister, fieldType);
		}
	}

	@Override
	protected void configureGeneratedIdSequence(StringBuilder sb, FieldType fieldType, List<String> statementsBefore,
			List<String> additionalArgs, List<String> queriesAfter) {
		String seqName = fieldType.getGeneratedIdSequence();
		// needs to match dropColumnArg()
		StringBuilder seqSb = new StringBuilder(64);
		seqSb.append("CREATE SEQUENCE ");
		// when it is created, it needs to be escaped specially
		appendEscapedEntityName(seqSb, seqName);
		statementsBefore.add(seqSb.toString());

		configureId(sb, fieldType, statementsBefore, additionalArgs, queriesAfter);
	}

	@Override
	protected void configureId(StringBuilder sb, FieldType fieldType, List<String> statementsBefore,
			List<String> additionalArgs, List<String> queriesAfter) {
		// no PRIMARY KEY per stew
	}

	@Override
	public void dropColumnArg(FieldType fieldType, List<String> statementsBefore, List<String> statementsAfter) {
		if (fieldType.isGeneratedIdSequence()) {
			StringBuilder sb = new StringBuilder(64);
			sb.append("DROP SEQUENCE ");
			appendEscapedEntityName(sb, fieldType.getGeneratedIdSequence());
			statementsAfter.add(sb.toString());
		}
	}

	@Override
	public void appendEscapedEntityName(StringBuilder sb, String name) {
		sb.append('\"').append(name).append('\"');
	}

	@Override
	public boolean isIdSequenceNeeded() {
		return true;
	}

	@Override
	public void appendSelectNextValFromSequence(StringBuilder sb, String sequenceName) {
		sb.append("SELECT ");
		// this may not work -- may need to have no escape
		appendEscapedEntityName(sb, sequenceName);
		// dual is some sort of special internal table I think
		sb.append(".nextval FROM dual");
	}

	@Override
	public String getPingStatement() {
		return "SELECT 1 FROM DUAL";
	}

	@Override
	public boolean isOffsetSqlSupported() {
		// there is no easy way to do this in this database type
		return false;
	}

	@Override
	public boolean isBatchUseTransaction() {
		// from stew
		return true;
	}

	@Override
	public boolean isSelectSequenceBeforeInsert() {
		// from stew
		return true;
	}

	@Override
	public boolean isEntityNamesMustBeUpCase() {
		// from stew
		return true;
	}
}
