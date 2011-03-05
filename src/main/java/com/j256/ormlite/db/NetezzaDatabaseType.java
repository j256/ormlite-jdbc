package com.j256.ormlite.db;

import java.util.List;

import com.j256.ormlite.field.FieldType;

/**
 * Netezza database type information used to create the tables, etc..
 * 
 * <p>
 * <b>NOTE:</b> This is the initial take on this database type. We hope to get access to an external database for
 * testing.
 * </p>
 * 
 * @author Richard Kooijman
 */
public class NetezzaDatabaseType extends BaseDatabaseType implements DatabaseType {

	private final static String DATABASE_URL_PORTION = "netezza";
	private final static String DRIVER_CLASS_NAME = "org.netezza.Driver";
	private final static String DATABASE_NAME = "Netezza";

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
	protected void appendByteType(StringBuilder sb) {
		sb.append("BYTEINT");
	}

	@Override
	protected void configureGeneratedIdSequence(StringBuilder sb, FieldType fieldType, List<String> statementsBefore,
			List<String> additionalArgs, List<String> queriesAfter) {
		String sequenceName = fieldType.getGeneratedIdSequence();
		// needs to match dropColumnArg()
		StringBuilder seqSb = new StringBuilder();
		seqSb.append("CREATE SEQUENCE ");
		// when it is created, it needs to be escaped specially
		appendEscapedEntityName(seqSb, sequenceName);
		statementsBefore.add(seqSb.toString());

		configureId(sb, fieldType, statementsBefore, additionalArgs, queriesAfter);
	}

	@Override
	public void dropColumnArg(FieldType fieldType, List<String> statementsBefore, List<String> statementsAfter) {
		if (fieldType.isGeneratedIdSequence()) {
			StringBuilder sb = new StringBuilder();
			sb.append("DROP SEQUENCE ");
			appendEscapedEntityName(sb, fieldType.getGeneratedIdSequence());
			statementsAfter.add(sb.toString());
		}
	}

	@Override
	public void appendEscapedEntityName(StringBuilder sb, String word) {
		sb.append('\"').append(word).append('\"');
	}

	@Override
	public boolean isIdSequenceNeeded() {
		return true;
	}

	@Override
	public void appendSelectNextValFromSequence(StringBuilder sb, String sequenceName) {
		sb.append("SELECT NEXT VALUE FOR ");
		// this is word and not entity unfortunately
		appendEscapedWord(sb, sequenceName);
	}
}
