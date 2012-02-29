package com.j256.ormlite.examples.datapersister;

import java.util.Date;

import org.joda.time.DateTime;

import com.j256.ormlite.field.DataPersisterManager;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Example user object that is persisted to disk by the DAO and other example classes.
 */
@DatabaseTable
public class User {

	// for UpdateBuilder to be able to find the fields
	public static final String FIELD_BIRTH_DATE = "birthDate";

	@DatabaseField(generatedId = true)
	private int id;

	@DatabaseField
	private String name;

	/**
	 * We use the persisterClass here to use our customer persister for handling invalid SQL Timestamp values.
	 */
	@DatabaseField(columnName = FIELD_BIRTH_DATE, persisterClass = MyDatePersister.class)
	private Date birthDate;

	/**
	 * NOTE: this is _not_ a default type that is stored by ORMLite so we are going to define a custom persister for
	 * {@link DateTime} and register it using
	 * {@link DataPersisterManager#registerDataPersisters(com.j256.ormlite.field.DataPersister...)}.
	 */
	@DatabaseField
	private DateTime createDateTime;

	User() {
		// all persisted classes must define a no-arg constructor with at least package visibility
	}

	public User(String name, Date birthDate, DateTime createDateTime) {
		this.name = name;
		this.birthDate = birthDate;
		this.createDateTime = createDateTime;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public Date getBirthDate() {
		return birthDate;
	}

	public DateTime getCreateDateTime() {
		return createDateTime;
	}
}
