package com.j256.ormlite.examples.datapersister;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Date;

import org.joda.time.DateTime;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.field.DataPersisterManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.UpdateBuilder;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;

/**
 * Main sample routine to show how to define custom data persisters for tuning how ORMLite writes and reads stuff from
 * the database.
 * 
 * <p>
 * <b>NOTE:</b> We use asserts in a couple of places to verify the results but if this were actual production code, we
 * would have proper error handling.
 * </p>
 */
public class DataPersisterMain {

	// we are using the in-memory H2 database
	private final static String DATABASE_URL = "jdbc:h2:mem:user";

	private Dao<User, Integer> userDao;

	public static void main(String[] args) throws Exception {
		// turn our static method into an instance of Main
		new DataPersisterMain().doMain(args);
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

		/*
		 * We register our own persister for DateTime objects. ORMLite actually has a built in one but it has to use
		 * reflection.
		 */
		DataPersisterManager.registerDataPersisters(DateTimePersister.getSingleton());

		userDao = DaoManager.createDao(connectionSource, User.class);

		// if you need to create the table
		TableUtils.createTable(connectionSource, User.class);
	}

	/**
	 * Read and write some example data.
	 */
	private void readWriteData() throws Exception {
		// create an instance of User
		String name = "Jim Coakley";
		Date birthDate = new Date();
		DateTime createDateTime = new DateTime().plusDays(10);
		User user = new User(name, birthDate, createDateTime);

		// persist the user object to the database
		userDao.create(user);

		// if we get the user from the database then we should
		User result = userDao.queryForId(user.getId());
		// our result birth-date should now be null because it is too early
		assertEquals(birthDate, result.getBirthDate());
		assertEquals(createDateTime, result.getCreateDateTime());

		// to simulate a 'zero-date' we update the database by hand
		UpdateBuilder<User, Integer> ub = userDao.updateBuilder();
		// set it to some silly value
		ub.updateColumnExpression(User.FIELD_BIRTH_DATE, "'0000-01-01'");
		assertEquals(1, ub.update());

		// now we pull back out the user to see if we get a null birth-date
		result = userDao.queryForId(user.getId());
		// our result birth-date should now be null because it is too early
		assertNull(result.getBirthDate());
	}
}
