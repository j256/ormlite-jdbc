package com.j256.ormlite.db;

import com.j256.ormlite.field.DataPersister;
import com.j256.ormlite.field.FieldConverter;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.jdbc.CharacterCompatFieldConverter;

/**
 * MariaDB database type information used to create the tables, etc.. It is an extension of MySQL.
 * 
 * @author kratorius
 */
public class MariaDbDatabaseType extends MysqlDatabaseType {

	private final static String DATABASE_URL_PORTION = "mariadb";
	private final static String DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver";
	private final static String DATABASE_NAME = "MariaDB";

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
	public FieldConverter getFieldConverter(DataPersister dataPersister, FieldType fieldType) {
		if (dataPersister.getSqlType() == SqlType.CHAR)
			return new CharacterCompatFieldConverter(dataPersister);

		return super.getFieldConverter(dataPersister, fieldType);
	}
}
