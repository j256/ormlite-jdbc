package com.j256.ormlite.examples.fieldConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Date;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.field.DatabaseFieldConfig;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTableConfig;
import com.j256.ormlite.table.TableUtils;

/**
 * Main sample routine to show how to do basic operations with the package.
 * 
 * <p>
 * <b>NOTE:</b> We use asserts in a couple of places to verify the results but if this were actual production code, we
 * would have proper error handling.
 * </p>
 */
public class FieldConfigMain {

	// we are using the in-memory H2 database
	private final static String DATABASE_URL = "jdbc:h2:mem:account";

	private Dao<Account, Integer> accountDao;
	private Dao<Delivery, Integer> deliveryDao;

	public static void main(String[] args) throws Exception {
		// turn our static method into an instance of Main
		new FieldConfigMain().doMain(args);
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

		DatabaseType databaseType = connectionSource.getDatabaseType();
		DatabaseTableConfig<Account> accountTableConfig = buildAccountTableConfig(databaseType);
		accountDao = DaoManager.createDao(connectionSource, accountTableConfig);

		DatabaseTableConfig<Delivery> deliveryTableConfig = buildDeliveryTableConfig(databaseType, accountTableConfig);
		deliveryDao = DaoManager.createDao(connectionSource, deliveryTableConfig);

		// if you need to create the table
		TableUtils.createTable(connectionSource, accountTableConfig);
		TableUtils.createTable(connectionSource, deliveryTableConfig);
	}

	private DatabaseTableConfig<Account> buildAccountTableConfig(DatabaseType databaseType) {
		ArrayList<DatabaseFieldConfig> fieldConfigs = new ArrayList<DatabaseFieldConfig>();
		DatabaseFieldConfig fieldConfig = new DatabaseFieldConfig("id");
		fieldConfig.setGeneratedId(true);
		fieldConfigs.add(fieldConfig);
		fieldConfigs.add(new DatabaseFieldConfig("name"));
		fieldConfig = new DatabaseFieldConfig("password");
		fieldConfig.setCanBeNull(true);
		fieldConfigs.add(fieldConfig);
		DatabaseTableConfig<Account> tableConfig =
				new DatabaseTableConfig<Account>(databaseType, Account.class, fieldConfigs);
		return tableConfig;
	}

	private DatabaseTableConfig<Delivery> buildDeliveryTableConfig(DatabaseType databaseType,
			DatabaseTableConfig<Account> accountTableConfig) {
		ArrayList<DatabaseFieldConfig> fieldConfigs = new ArrayList<DatabaseFieldConfig>();
		DatabaseFieldConfig fieldConfig = new DatabaseFieldConfig("id");
		fieldConfig.setGeneratedId(true);
		fieldConfigs.add(fieldConfig);
		fieldConfigs.add(new DatabaseFieldConfig("when"));
		fieldConfigs.add(new DatabaseFieldConfig("signedBy"));
		fieldConfig = new DatabaseFieldConfig("account");
		fieldConfig.setForeign(true);
		fieldConfig.setForeignTableConfig(accountTableConfig);
		fieldConfigs.add(fieldConfig);
		DatabaseTableConfig<Delivery> tableConfig =
				new DatabaseTableConfig<Delivery>(databaseType, Delivery.class, fieldConfigs);
		return tableConfig;
	}

	/**
	 * Read and write some example data.
	 */
	private void readWriteData() throws Exception {
		// create an instance of Account
		String name = "Jim Coakley";
		Account account = new Account(name);
		// persist the account object to the database
		accountDao.create(account);

		Delivery delivery = new Delivery(new Date(), "Mr. Ed", account);
		// persist the account object to the database
		deliveryDao.create(delivery);

		Delivery delivery2 = deliveryDao.queryForId(delivery.getId());
		assertNotNull(delivery2);
		assertEquals(delivery.getId(), delivery2.getId());
		assertEquals(account.getId(), delivery2.getAccount().getId());
	}
}
