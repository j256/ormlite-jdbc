package com.j256.ormlite.db;

import java.util.List;

import com.j256.ormlite.field.FieldType;

/**
 * Postgres database type information used to create the tables, etc..
 * 
 * @author graywatson
 */
public class PostgresDatabaseType extends BaseDatabaseType {

	private final static String DATABASE_URL_PORTION = "postgresql";
	private final static String DRIVER_CLASS_NAME = "org.postgresql.Driver";
	private final static String DATABASE_NAME = "Postgres";

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
	protected void appendUuidNativeType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("UUID");
	}

	@Override
	protected void appendByteType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("SMALLINT");
	}

	@Override
	protected void appendByteArrayType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("BYTEA");
	}

	@Override
	protected void appendSerializableType(StringBuilder sb, FieldType fieldType, int fieldWidth) {
		sb.append("BYTEA");
	}

	@Override
	protected void configureGeneratedIdSequence(StringBuilder sb, FieldType fieldType, List<String> statementsBefore,
			List<String> additionalArgs, List<String> queriesAfter) {
		String sequenceName = fieldType.getGeneratedIdSequence();
		// needs to match dropColumnArg()
		StringBuilder seqSb = new StringBuilder(64);
		seqSb.append("CREATE SEQUENCE ");
		// when it is created, it needs to be escaped specially
		appendEscapedEntityName(seqSb, sequenceName);
		statementsBefore.add(seqSb.toString());

		sb.append("DEFAULT NEXTVAL(");
		// postgres needed this special escaping for NEXTVAL('"sequence-name"')
		sb.append('\'').append('\"').append(sequenceName).append('\"').append('\'');
		sb.append(") ");
		// could also be the type serial for auto-generated sequences
		// 8.2 also have the returning insert statement

		configureId(sb, fieldType, statementsBefore, additionalArgs, queriesAfter);
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
		// this handles table names like schema.table which have to be quoted like "schema"."table"
		boolean first = true;
		for (String namePart : name.split("\\.")) {
			if (first) {
				first = false;
			} else {
				sb.append('.');
			}
			sb.append('\"').append(namePart).append('\"');
		}
	}

	@Override
	public boolean isIdSequenceNeeded() {
		return true;
	}

	@Override
	public boolean isSelectSequenceBeforeInsert() {
		return true;
	}

	@Override
	public void appendSelectNextValFromSequence(StringBuilder sb, String sequenceName) {
		sb.append("SELECT NEXTVAL(");
		// this is word and not entity unfortunately
		appendEscapedWord(sb, sequenceName);
		sb.append(')');
	}

	@Override
	public boolean isTruncateSupported() {
		return true;
	}

	@Override
	public boolean isCreateIfNotExistsSupported() {
		int major = driver.getMajorVersion();
		if (major > 9 || (major == 9 && driver.getMinorVersion() >= 1)) {
			return true;
		} else {
			return super.isCreateIfNotExistsSupported();
		}
	}
}
