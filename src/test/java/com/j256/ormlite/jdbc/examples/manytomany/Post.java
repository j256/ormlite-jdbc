package com.j256.ormlite.jdbc.examples.manytomany;

import com.j256.ormlite.field.DatabaseField;

/**
 * Post to some blog with String content.
 */
public class Post {

	// we use this field-name so we can query for posts with a certain id
	public final static String ID_FIELD_NAME = "id";

	// this id is generated by the database and set on the object when it is passed to the create method
	@DatabaseField(generatedId = true, columnName = ID_FIELD_NAME)
	int id;

	// contents of the post
	@DatabaseField
	String contents;

	Post() {
		// for ormlite
	}

	public Post(String contents) {
		this.contents = contents;
	}
}
