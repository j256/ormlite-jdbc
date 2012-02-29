package com.j256.ormlite.examples.foreign;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;

/**
 * Main sample routine to show how to do basic operations with the package.
 * 
 * <p>
 * <b>NOTE:</b> We use asserts in a couple of places to verify the results but if this were actual production code, we
 * would have proper error handling.
 * </p>
 */
public class ForeignMain {

	// we are using the in-memory H2 database
	private final static String DATABASE_URL = "jdbc:h2:mem:account";

	private Dao<Account, Integer> accountDao;
	private Dao<Order, Integer> orderDao;

	public static void main(String[] args) throws Exception {
		// turn our static method into an instance of Main
		new ForeignMain().doMain(args);
	}

	private void doMain(String[] args) throws Exception {
		JdbcConnectionSource connectionSource = null;
		try {
			// create our data source
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

		accountDao = DaoManager.createDao(connectionSource, Account.class);
		orderDao = DaoManager.createDao(connectionSource, Order.class);

		// if you need to create the table
		TableUtils.createTable(connectionSource, Account.class);
		TableUtils.createTable(connectionSource, Order.class);
	}

	private void readWriteData() throws Exception {
		// create an instance of Account
		String name = "Buzz Lightyear";
		Account account = new Account(name);

		// persist the account object to the database
		accountDao.create(account);

		// create an associated Order for the Account
		// Buzz bought 2 of item #21312 for a price of $12.32
		int quantity1 = 2;
		int itemNumber1 = 21312;
		float price1 = 12.32F;
		Order order1 = new Order(account, itemNumber1, price1, quantity1);
		orderDao.create(order1);

		// create another Order for the Account
		// Buzz also bought 1 of item #785 for a price of $7.98
		int quantity2 = 1;
		int itemNumber2 = 785;
		float price2 = 7.98F;
		Order order2 = new Order(account, itemNumber2, price2, quantity2);
		orderDao.create(order2);

		// construct a query using the QueryBuilder
		QueryBuilder<Order, Integer> statementBuilder = orderDao.queryBuilder();
		// should find both of the orders that match the account
		// ORMLite extracts the id from the account for the query automagically
		statementBuilder.where().eq(Order.ACCOUNT_ID_FIELD_NAME, account);
		List<Order> orders = orderDao.query(statementBuilder.prepare());

		// sanity checks
		assertEquals("Should have found both of the orders for the account", 2, orders.size());
		assertTrue(orderDao.objectsEqual(order1, orders.get(0)));
		assertTrue(orderDao.objectsEqual(order2, orders.get(1)));

		/*
		 * Notice that in each of the orders that we got from the query, the Account id is good but the name field is
		 * null. With foreign object fields, only the id field is stored in the table for the order.
		 */
		assertEquals(account.getId(), orders.get(0).getAccount().getId());
		assertEquals(account.getId(), orders.get(1).getAccount().getId());
		assertNull(orders.get(0).getAccount().getName());
		assertNull(orders.get(1).getAccount().getName());

		/*
		 * To get the name field from the order's account field, we need to refresh each of the objects in the list
		 * which will lookup the id and load in all of the fields.
		 */
		assertEquals(1, accountDao.refresh(orders.get(0).getAccount()));
		assertEquals(1, accountDao.refresh(orders.get(1).getAccount()));

		// now the account name field has been filled in
		assertEquals(account.getName(), orders.get(0).getAccount().getName());
		assertEquals(account.getName(), orders.get(1).getAccount().getName());
	}
}
