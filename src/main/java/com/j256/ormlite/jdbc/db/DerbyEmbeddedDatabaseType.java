package com.j256.ormlite.jdbc.db;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import com.j256.ormlite.db.BaseDatabaseType;
import com.j256.ormlite.field.BaseFieldConverter;
import com.j256.ormlite.field.DataPersister;
import com.j256.ormlite.field.FieldConverter;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.field.converter.BooleanNumberFieldConverter;
import com.j256.ormlite.field.types.BooleanCharType;
import com.j256.ormlite.misc.IOUtils;
import com.j256.ormlite.support.DatabaseResults;

/**
 * Derby database type information used to create the tables, etc.. This is for an embedded Derby database. For client
 * connections to a remote Derby server, you should use {@link DerbyClientServerDatabaseType}.
 * 
 * @author graywatson
 */
public class DerbyEmbeddedDatabaseType extends BaseDatabaseType {

	protected final static String DATABASE_URL_PORTION = "derby";
	private final static String DRIVER_CLASS_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	private final static String DATABASE_NAME = "Derby";

	@Override
	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		if (!DATABASE_URL_PORTION.equals(dbTypePart)) {
			return false;
		}
		// jdbc:derby:sample;
		String[] parts = url.split(":");
		return (parts.length >= 3 && !parts[2].startsWith("//"));
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
	public FieldConverter getFieldConverter(DataPersister dataType, FieldType fieldType) {
		// we are only overriding certain types
		switch (dataType.getSqlType()) {
			case BOOLEAN:
				return BooleanNumberFieldConverter.getSingleton();
			case CHAR:
				if (dataType instanceof BooleanCharType) {
					return BooleanNumberFieldConverter.getSingleton();
				} else {
					return CharFieldConverter.getSingleton();
				}
			case SERIALIZABLE:
				return SerializableFieldConverter.getSingleton();
			default:
				return super.getFieldConverter(dataType, fieldType);
		}
	}

	@Override
	protected void appendLongStringType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("LONG VARCHAR");
	}

	@Override
	public void appendOffsetValue(StringBuilder sb, long offset) {
		// I love the required ROWS prefix. Hilarious.
		sb.append("OFFSET ").append(offset).append(" ROWS ");
	}

	@Override
	protected void appendBooleanType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		// I tried "char for bit data" and "char(1)" with no luck
		sb.append("SMALLINT");
	}

	@Override
	protected void appendCharType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("SMALLINT");
	}

	@Override
	protected void appendByteType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("SMALLINT");
	}

	@Override
	protected void appendByteArrayType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("LONG VARCHAR FOR BIT DATA");
	}

	@Override
	protected void configureGeneratedId(String tableName, StringBuilder sb, FieldType fieldType,
			List<String> statementsBefore, List<String> statementsAfter, List<String> additionalArgs,
			List<String> queriesAfter) {
		sb.append("GENERATED BY DEFAULT AS IDENTITY ");
		configureId(sb, fieldType, statementsBefore, additionalArgs, queriesAfter);
	}

	@Override
	public void appendEscapedEntityName(StringBuilder sb, String name) {
		sb.append('\"').append(name).append('\"');
	}

	@Override
	public boolean isLimitSqlSupported() {
		return false;
	}

	@Override
	public String getPingStatement() {
		return "SELECT 1 FROM SYSIBM.SYSDUMMY1";
	}

	@Override
	public boolean isEntityNamesMustBeUpCase() {
		return true;
	}

	@Override
	public boolean isAllowGeneratedIdInsertSupported() {
		/*
		 * This is unfortunate but Derby does not allow me to insert a null into a generated-id field. Everyone else
		 * does of course.
		 */
		return false;
	}

	@Override
	public void appendInsertNoColumns(StringBuilder sb) {
		sb.append("VALUES(DEFAULT)");
	}

	@Override
	protected void appendSerializableType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		super.appendSerializableType(sb, fieldType, fieldWidth);
	}

	/**
	 * Conversion from the Object Java field to the BLOB Jdbc type because the varbinary needs a size otherwise.
	 */
	private static class SerializableFieldConverter extends BaseFieldConverter {

		private static final SerializableFieldConverter singleTon = new SerializableFieldConverter();

		public static SerializableFieldConverter getSingleton() {
			return singleTon;
		}

		@Override
		public SqlType getSqlType() {
			return SqlType.BLOB;
		}

		@Override
		public Object parseDefaultString(FieldType fieldType, String defaultStr) throws SQLException {
			throw new SQLException("Default values for serializable types are not supported");
		}

		@Override
		public Object resultToSqlArg(FieldType fieldType, DatabaseResults results, int columnPos) throws SQLException {
			return results.getBlobStream(columnPos);
		}

		@Override
		public Object sqlArgToJava(FieldType fieldType, Object sqlArg, int columnPos) throws SQLException {
			InputStream stream = (InputStream) sqlArg;
			try {
				ObjectInputStream objInStream = new ObjectInputStream(stream);
				return objInStream.readObject();
			} catch (Exception e) {
				throw new SQLException("Could not read serialized object from result blob", e);
			} finally {
				IOUtils.closeQuietly(stream);
			}
		}

		@Override
		public Object javaToSqlArg(FieldType fieldType, Object javaObject) throws SQLException {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			try {
				ObjectOutputStream objOutStream = new ObjectOutputStream(outStream);
				objOutStream.writeObject(javaObject);
			} catch (Exception e) {
				throw new SQLException("Could not write serialized object to output stream", e);
			}
			return new SerialBlob(outStream.toByteArray());
		}

		@Override
		public boolean isStreamType() {
			return true;
		}

		@Override
		public Object resultStringToJava(FieldType fieldType, String stringValue, int columnPos) throws SQLException {
			throw new SQLException("Parsing string value for serializable types is not supported");
		}
	}

	/**
	 * Conversion from the char Java field because Derby can't convert Character to type char. Jesus.
	 */
	private static class CharFieldConverter extends BaseFieldConverter {

		private static final CharFieldConverter singleTon = new CharFieldConverter();

		public static CharFieldConverter getSingleton() {
			return singleTon;
		}

		@Override
		public SqlType getSqlType() {
			return SqlType.INTEGER;
		}

		@Override
		public Object javaToSqlArg(FieldType fieldType, Object javaObject) {
			char character = (char) (Character) javaObject;
			return (int) character;
		}

		@Override
		public Object parseDefaultString(FieldType fieldType, String defaultStr) throws SQLException {
			if (defaultStr.length() != 1) {
				throw new SQLException(
						"Problems with field " + fieldType + ", default string to long: '" + defaultStr + "'");
			}
			return (int) defaultStr.charAt(0);
		}

		@Override
		public Object resultToSqlArg(FieldType fieldType, DatabaseResults results, int columnPos) throws SQLException {
			return results.getInt(columnPos);
		}

		@Override
		public Object sqlArgToJava(FieldType fieldType, Object sqlArg, int columnPos) {
			int intVal = (Integer) sqlArg;
			return (char) intVal;
		}

		@Override
		public Object resultStringToJava(FieldType fieldType, String stringValue, int columnPos) {
			return sqlArgToJava(fieldType, Integer.parseInt(stringValue), columnPos);
		}
	}
}
