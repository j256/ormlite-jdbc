package com.j256.ormlite.examples.foreignCollection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;

import com.j256.ormlite.dao.CloseableIterator;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
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
public class ForeignCollectionMain {

	// we are using the in-memory H2 database
	private final static String DATABASE_URL = "jdbc:h2:mem:account";

	private Dao<Account, Integer> accountDao;
	private Dao<Order, Integer> orderDao;

	public static void main(String[] args) throws Exception {
		// turn our static method into an instance of Main
		new ForeignCollectionMain().doMain(args);
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

		Account accountResult = accountDao.queryForId(account.getId());
		ForeignCollection<Order> orders = accountResult.getOrders();

		// sanity checks
		CloseableIterator<Order> iterator = orders.closeableIterator();
		try {
			assertTrue(iterator.hasNext());
			Order order = iterator.next();
			assertEquals(itemNumber1, order.getItemNumber());
			assertSame(accountResult, order.getAccount());
			assertTrue(iterator.hasNext());
			order = iterator.next();
			assertEquals(itemNumber2, order.getItemNumber());
			assertFalse(iterator.hasNext());
		} finally {
			// must always close our iterators otherwise connections to the database are held open
			iterator.close();
		}

		// create another Order for the Account
		// Buzz also bought 1 of item #785 for a price of $7.98
		int quantity3 = 50;
		int itemNumber3 = 78315;
		float price3 = 72.98F;
		Order order3 = new Order(account, itemNumber3, price3, quantity3);

		// now let's add this order via the foreign collection
		orders.add(order3);
		// now there are 3 of them in there
		assertEquals(3, orders.size());

		List<Order> orderList = orderDao.queryForAll();
		// and 3 in the database
		assertEquals(3, orderList.size());
	}
}
