package com.j256.ormlite.examples.manytomany;

import com.j256.ormlite.field.DatabaseField;

/**
 * Post to some blog.
 */
public class Post {

	public final static String ID_FIELD_NAME = "id";

	@DatabaseField(generatedId = true, columnName = ID_FIELD_NAME)
	int id;

	@DatabaseField
	String contents;

	Post() {
		// for ormlite
	}

	public Post(String contents) {
		this.contents = contents;
	}
}
