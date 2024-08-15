package com.j256.ormlite.jdbc.examples.manytomany;

import com.j256.ormlite.field.DatabaseField;

/**
 * Join table which links users to their posts.
 * 
 * <p>
 * For more information about foreign objects, see the <a href="http://ormlite.com/docs/foreign" >online docs</a>
 * </p>
 */
public class UserPost {

	public final static String USER_ID_FIELD_NAME = "user_id";
	public final static String POST_ID_FIELD_NAME = "post_id";

	/**
	 * This id is generated by the database and set on the object when it is passed to the create method. An id is
	 * needed in case we need to update or delete this object in the future.
	 */
	@DatabaseField(generatedId = true)
	int id;

	// This is a foreign object which just stores the id from the User object in this table.
	@DatabaseField(foreign = true, columnName = USER_ID_FIELD_NAME)
	User user;

	// This is a foreign object which just stores the id from the Post object in this table.
	@DatabaseField(foreign = true, columnName = POST_ID_FIELD_NAME)
	Post post;

	UserPost() {
		// for ormlite
	}

	public UserPost(User user, Post post) {
		this.user = user;
		this.post = post;
	}
}
