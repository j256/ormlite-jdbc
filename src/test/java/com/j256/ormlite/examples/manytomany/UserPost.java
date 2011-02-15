package com.j256.ormlite.examples.manytomany;

import com.j256.ormlite.field.DatabaseField;

/**
 * Join table which links users to their posts.
 */
public class UserPost {

	public final static String USER_FIELD_NAME = "user_id";
	public final static String POST_FIELD_NAME = "post_id";
	
	@DatabaseField(generatedId = true)
	int id;

	@DatabaseField(foreign = true, columnName = USER_FIELD_NAME)
	User user;

	@DatabaseField(foreign = true, columnName = POST_FIELD_NAME)
	Post post;

	UserPost() {
		// for ormlite
	}

	public UserPost(User user, Post post) {
		this.user = user;
		this.post = post;
	}
}
