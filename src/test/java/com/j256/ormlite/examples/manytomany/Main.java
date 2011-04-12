package com.j256.ormlite.examples.manytomany;

import static org.junit.Assert.assertEquals;

import java.util.List;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.PreparedQuery;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.stmt.SelectArg;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;

/**
 * Main sample routine to show how to do many-to-many type relationships. It also demonstrates how we user inner queries
 * as well foreign objects.
 * 
 * <p>
 * <b>NOTE:</b> We use asserts in a couple of places to verify the results but if this were actual production code, we
 * would have proper error handling.
 * </p>
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
			setupDatabase(connectionSource);
			// read and write some data
			readWriteData();
			System.out.println("\n\nIt seems to have worked\n\n");
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
	private void setupDatabase(ConnectionSource connectionSource) throws Exception {

		/**
		 * Create our DAOs. One for each class and associated table.
		 */
		userDao = DaoManager.createDao(connectionSource, User.class);
		postDao = DaoManager.createDao(connectionSource, Post.class);
		userPostDao = DaoManager.createDao(connectionSource, UserPost.class);

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

		// persist the user object to the database
		userDao.create(user1);

		// have user1 post something
		Post post1 = new Post("Wow is it cold outside!!");
		// save the post to the post table
		postDao.create(post1);

		// link the user and the post together in the join table
		UserPost user1Post1 = new UserPost(user1, post1);
		userPostDao.create(user1Post1);

		// have user1 post a second post
		Post post2 = new Post("Now it's a bit warmer thank goodness.");
		postDao.create(post2);
		UserPost user1Post2 = new UserPost(user1, post2);
		userPostDao.create(user1Post2);

		// create another user
		User user2 = new User("Rose Gray");
		userDao.create(user2);

		// have the 2nd user also say the 2nd post
		UserPost user2Post1 = new UserPost(user2, post2);
		userPostDao.create(user2Post1);

		/*
		 * Now go back and do various queries to look things up.
		 */

		/*
		 * show me all of a user's posts:
		 */
		// build our inner query for UserPost objects
		QueryBuilder<UserPost, Integer> userPostQb = userPostDao.queryBuilder();
		// just select the post-id field
		userPostQb.selectColumns(UserPost.POST_ID_FIELD_NAME);
		SelectArg userSelectArg = new SelectArg();
		// you could also just pass in user1 here
		userPostQb.where().eq(UserPost.USER_ID_FIELD_NAME, userSelectArg);

		// build our outer query for Post objects
		QueryBuilder<Post, Integer> postQb = postDao.queryBuilder();
		// where the id matches in the post-id from the inner query
		postQb.where().in(Post.ID_FIELD_NAME, userPostQb);
		PreparedQuery<Post> postPrepared = postQb.prepare();

		// user1 should have 2 posts
		userSelectArg.setValue(user1);
		List<Post> posts = postDao.query(postPrepared);
		assertEquals(2, posts.size());
		assertEquals(user1Post1.post.id, posts.get(0).id);
		assertEquals(user1Post2.post.id, posts.get(1).id);

		// user2 should have only 1 post
		userSelectArg.setValue(user2);
		// posts = postDao.query(postQb.prepare());
		posts = postDao.query(postPrepared);
		assertEquals(1, posts.size());
		assertEquals(user2Post1.post.id, posts.get(0).id);

		/*
		 * show me all of the users that have a post.
		 */
		// build our next inner query
		userPostQb = userPostDao.queryBuilder();
		// this time selecting for the user-id field
		userPostQb.selectColumns(UserPost.USER_ID_FIELD_NAME);
		SelectArg postSelectArg = new SelectArg();
		userPostQb.where().eq(UserPost.POST_ID_FIELD_NAME, postSelectArg);

		// build our outer query
		QueryBuilder<User, Integer> userQb = userDao.queryBuilder();
		// where the user-id matches the inner query's user-id field
		userQb.where().in(Post.ID_FIELD_NAME, userPostQb);
		PreparedQuery<User> userPrepared = userQb.prepare();

		// post1 should only have 1 corresponding user
		postSelectArg.setValue(post1);
		List<User> users = userDao.query(userPrepared);
		assertEquals(1, users.size());
		assertEquals(user1.id, users.get(0).id);

		// post2 should have 2 corresponding users
		postSelectArg.setValue(post2);
		users = userDao.query(userPrepared);
		assertEquals(2, users.size());
		assertEquals(user1.id, users.get(0).id);
		assertEquals(user2.id, users.get(1).id);
	}
}
