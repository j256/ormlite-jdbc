package com.j256.ormlite.examples.manytomany;

import static org.junit.Assert.assertEquals;

import java.util.List;

import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.PreparedQuery;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.stmt.SelectArg;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;

/**
 * Main sample routine to show how to do many-to-many type relationships. It also demonstrates how we user inner queries
 * as well foreign objects.
 */
public class Main {

	// we are using the in-memory H2 database
	private final static String DATABASE_URL = "jdbc:h2:mem:manytomany";

	private Dao<User, Integer> userDao;
	private Dao<Post, Integer> postDao;
	private Dao<UserPost, Integer> userPostDao;

	public static void main(String[] args) throws Exception {
		// turn our static method into an instance of Main
		new Main().doMain(args);
	}

	private void doMain(String[] args) throws Exception {
		JdbcConnectionSource connectionSource = null;
		try {
			// create our data-source for the database
			connectionSource = new JdbcConnectionSource(DATABASE_URL);
			// setup our database and DAOs
			setupDatabase(DATABASE_URL, connectionSource);
			// read and write some data
			readWriteData();
		} finally {
			// destroy the data source which should close underlying connections
			if (connectionSource != null) {
				connectionSource.close();
			}
		}
	}

	/**
	 * Setup our database and DAOs
	 */
	private void setupDatabase(String databaseUrl, ConnectionSource connectionSource) throws Exception {

		/**
		 * Creare our DAOs. One for each class and associated table.
		 */
		userDao = BaseDaoImpl.createDao(connectionSource, User.class);
		postDao = BaseDaoImpl.createDao(connectionSource, Post.class);
		userPostDao = BaseDaoImpl.createDao(connectionSource, UserPost.class);

		/**
		 * Create the tables for our example. This would not be necessary if the tables already existed.
		 */
		TableUtils.createTable(connectionSource, User.class);
		TableUtils.createTable(connectionSource, Post.class);
		TableUtils.createTable(connectionSource, UserPost.class);
	}

	/**
	 * Read and write some example data.
	 */
	private void readWriteData() throws Exception {

		// create our 1st user
		User user1 = new User("Jim Coakley");

		// persist the user object to the database, it should return 1 row changed
		if (userDao.create(user1) != 1) {
			throw new Exception("Could not create user in database");
		}

		// have user1 post something
		Post post1 = new Post("Wow is it cold outside!!");
		// save the post to the post table
		if (postDao.create(post1) != 1) {
			throw new Exception("Could not create post in database");
		}
		// link the user and the post together in the join table
		UserPost userPost1 = new UserPost(user1, post1);
		if (userPostDao.create(userPost1) != 1) {
			throw new Exception("Could not create userPost in database");
		}

		// have user1 post a second post
		Post post2 = new Post("Now it's a bit warmer thank goodness.");
		if (postDao.create(post2) != 1) {
			throw new Exception("Could not create post in database");
		}
		UserPost userPost2 = new UserPost(user1, post2);
		if (userPostDao.create(userPost2) != 1) {
			throw new Exception("Could not create userPost in database");
		}

		// create another user
		User user2 = new User("Rose Gray");
		if (userDao.create(user2) != 1) {
			throw new Exception("Could not create user in database");
		}

		// have the 2nd user also say the 2nd post
		UserPost userPost3 = new UserPost(user2, post2);
		if (userPostDao.create(userPost3) != 1) {
			throw new Exception("Could not create userPost in database");
		}

		/*
		 * Now go back and do various queries to look things up.
		 */

		/*
		 * show me all of a user's posts:
		 */
		QueryBuilder<UserPost, Integer> userPostQb = userPostDao.queryBuilder();
		userPostQb.selectColumns(UserPost.POST_ID_FIELD_NAME);
		// you could also just pass in post1 here
		SelectArg userSelectArg = new SelectArg();
		userPostQb.where().eq(UserPost.USER_ID_FIELD_NAME, userSelectArg);

		QueryBuilder<Post, Integer> postQb = postDao.queryBuilder();
		postQb.where().in(Post.ID_FIELD_NAME, userPostQb);
		PreparedQuery<Post> postPrepared = postQb.prepare();

		// user1 should have 2 posts
		userSelectArg.setValue(user1);
		List<Post> posts = postDao.query(postPrepared);
		assertEquals(2, posts.size());

		// user2 should have only 1 post
		userSelectArg.setValue(user2);
		// posts = postDao.query(postQb.prepare());
		posts = postDao.query(postPrepared);
		assertEquals(1, posts.size());

		/*
		 * show me all of the users that have a post.
		 */
		userPostQb = userPostDao.queryBuilder();
		userPostQb.selectColumns(UserPost.USER_ID_FIELD_NAME);
		SelectArg postSelectArg = new SelectArg();
		userPostQb.where().eq(UserPost.POST_ID_FIELD_NAME, postSelectArg);

		QueryBuilder<User, Integer> userQb = userDao.queryBuilder();
		userQb.where().in(Post.ID_FIELD_NAME, userPostQb);
		PreparedQuery<User> userPrepared = userQb.prepare();

		// post1 should only have 1 corresponding user
		postSelectArg.setValue(post1);
		List<User> users = userDao.query(userPrepared);
		assertEquals(1, users.size());

		// post2 should have 2 corresponding users
		postSelectArg.setValue(post2);
		users = userDao.query(userPrepared);
		assertEquals(2, users.size());
	}
}
