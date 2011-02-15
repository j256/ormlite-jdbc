package com.j256.ormlite.examples.manytomany;

import com.j256.ormlite.field.DatabaseField;

/**
 * Some user object.
 */
public class User {

	public final static String ID_FIELD_NAME = "id";

	@DatabaseField(generatedId = true, columnName = ID_FIELD_NAME)
	int id;

	@DatabaseField
	String name;

	User() {
		// for ormlite
	}

	public User(String name) {
		this.name = name;
	}
}
