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

	private final static FieldConverter booleanConverter = new BooleanFieldConverter();

	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		return DATABASE_URL_PORTION.equals(dbTypePart);
	}

	@Override
	protected String getDriverClassName() {
		return DRIVER_CLASS_NAME;
	}

	public String getDatabaseName() {
		return DATABASE_NAME;
	}

	@Override
	protected void appendStringType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("VARCHAR2(").append(fieldWidth).append(")");
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
	public FieldConverter getFieldConverter(DataPersister dataPersister) {
		switch (dataPersister.getSqlType()) {
			case BOOLEAN :
				return booleanConverter;
			default :
				return super.getFieldConverter(dataPersister);
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

	/**
	 * Booleans in Oracle are stored as the character '1' or '0'. You can change the characters by specifying a format
	 * string. It must be a string with 2 characters. The first character is the value for TRUE, the second is FALSE.
	 * 
	 * <p>
	 * 
	 * <pre>
	 * &#64;DatabaseField(format = "YN")
	 * </pre>
	 * 
	 * You can also specify the format as "integer" to use an integer column type and the value 1 (really non-0) for
	 * true and 0 for false:
	 * 
	 * <pre>
	 * &#64;DatabaseField(format = "integer")
	 * </pre>
	 * 
	 * </p>
	 * 
	 * Thanks much to stew.
	 */
	protected static class BooleanFieldConverter extends BaseFieldConverter {

		private static final String TRUE_FALSE_FORMAT = "10";

		public SqlType getSqlType() {
			return SqlType.CHAR;
		}

		public Object parseDefaultString(FieldType fieldType, String defaultStr) throws SQLException {
			return javaToSqlArg(fieldType, Boolean.parseBoolean(defaultStr));
		}

		@Override
		public Object javaToSqlArg(FieldType fieldType, Object obj) throws SQLException {
			String format = getTrueFalseFormat(fieldType);
			if (format == BOOLEAN_INTEGER_FORMAT) {
				return ((Boolean) obj ? Integer.valueOf(1) : Integer.valueOf(0));
			} else {
				return ((Boolean) obj ? format.charAt(0) : format.charAt(1));
			}
		}

		public Object resultToSqlArg(FieldType fieldType, DatabaseResults results, int columnPos) throws SQLException {
			return results.getChar(columnPos);
		}

		@Override
		public Object sqlArgToJava(FieldType fieldType, Object sqlArg, int columnPos) throws SQLException {
			String format = getTrueFalseFormat(fieldType);
			if (format == BOOLEAN_INTEGER_FORMAT) {
				return ((Integer) sqlArg == 0 ? Boolean.FALSE : Boolean.TRUE);
			} else {
				return ((Character) sqlArg == format.charAt(0) ? Boolean.TRUE : Boolean.FALSE);
			}
		}

		public Object resultStringToJava(FieldType fieldType, String stringValue, int columnPos) throws SQLException {
			if (stringValue.length() == 0) {
				return Boolean.FALSE;
			} else {
				return sqlArgToJava(fieldType, stringValue.charAt(0), columnPos);
			}
		}

		private String getTrueFalseFormat(FieldType fieldType) throws SQLException {
			String format = fieldType.getFormat();
			if (format == null) {
				return TRUE_FALSE_FORMAT;
			}
			if (format.equalsIgnoreCase(BOOLEAN_INTEGER_FORMAT)) {
				return BOOLEAN_INTEGER_FORMAT;
			} else if (format.length() == 2 && format.charAt(0) != format.charAt(1)) {
				return format;
			} else {
				throw new SQLException(
						"Invalid boolean format must have 2 different characters that represent true/false like \"10\": "
								+ format);
			}
		}
	}
}
