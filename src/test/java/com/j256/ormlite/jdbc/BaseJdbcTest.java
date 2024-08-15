package com.j256.ormlite.jdbc;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.table.DatabaseTableConfig;
import com.j256.ormlite.table.TableUtils;

public abstract class BaseJdbcTest {

	private static final String DATASOURCE_ERROR = "Property 'dataSource' is required";

	protected static final String DEFAULT_DATABASE_URL = "jdbc:h2:mem:ormlite";

	protected String databaseHost = null;
	protected String databaseUrl = DEFAULT_DATABASE_URL;
	protected String userName = null;
	protected String password = null;

	protected JdbcConnectionSource connectionSource = null;
	protected boolean isConnectionExpected = false;
	protected DatabaseType databaseType = null;

	private Set<Class<?>> dropClassSet = new HashSet<Class<?>>();
	private Set<DatabaseTableConfig<?>> dropTableConfigSet = new HashSet<DatabaseTableConfig<?>>();

	@BeforeEach
	public void before() throws Exception {
		DaoManager.clearCache();
		if (connectionSource != null) {
			return;
		}
		// do this for everyone
		System.setProperty("derby.stream.error.file", "target/derby.log");
		setDatabaseParams();
		doOpenConnectionSource();
	}

	protected void openConnectionSource() throws Exception {
		if (connectionSource == null) {
			doOpenConnectionSource();
		}
	}

	/**
	 * Set the database parameters for this db type.
	 * 
	 * @throws SQLException
	 *             For sub classes
	 */
	protected void setDatabaseParams() throws SQLException {
		// noop here -- designed to be overridden
	}

	@AfterEach
	public void after() throws Exception {
		if (connectionSource != null) {
			for (Class<?> clazz : dropClassSet) {
				dropTable(clazz, true);
			}
			dropClassSet.clear();
			for (DatabaseTableConfig<?> tableConfig : dropTableConfigSet) {
				dropTable(tableConfig, true);
			}
			dropTableConfigSet.clear();
		}
		closeConnectionSource();
		DaoManager.clearCache();
	}

	/**
	 * Return if this test was expecting to be able to load the driver class
	 */
	protected boolean isDriverClassExpected() {
		return true;
	}

	/**
	 * Return if this test was expecting to be able to connect to the database
	 */
	protected boolean isConnectionExpected() throws IOException {
		try {
			if (databaseHost == null) {
				return true;
			} else {
				return InetAddress.getByName(databaseHost).isReachable(500);
			}
		} catch (UnknownHostException e) {
			return false;
		}
	}

	protected void closeConnectionSource() throws Exception {
		if (connectionSource != null) {
			connectionSource.close();
			connectionSource = null;
		}
	}

	protected <T, ID> Dao<T, ID> createDao(Class<T> clazz, boolean createTable) throws Exception {
		if (connectionSource == null) {
			throw new SQLException(DATASOURCE_ERROR);
		}
		@SuppressWarnings("unchecked")
		BaseDaoImpl<T, ID> dao = (BaseDaoImpl<T, ID>) DaoManager.createDao(connectionSource, clazz);
		return configDao(dao, createTable);
	}

	protected <T, ID> Dao<T, ID> createDao(DatabaseTableConfig<T> tableConfig, boolean createTable) throws Exception {
		if (connectionSource == null) {
			throw new SQLException(DATASOURCE_ERROR);
		}
		@SuppressWarnings("unchecked")
		BaseDaoImpl<T, ID> dao = (BaseDaoImpl<T, ID>) DaoManager.createDao(connectionSource, tableConfig);
		return configDao(dao, createTable);
	}

	protected <T> void createTable(Class<T> clazz, boolean dropAtEnd) throws Exception {
		try {
			// first we drop it in case it existed before
			dropTable(clazz, true);
		} catch (SQLException ignored) {
			// ignore any errors about missing tables
		}
		TableUtils.createTable(connectionSource, clazz);
		if (dropAtEnd) {
			dropClassSet.add(clazz);
		}
	}

	protected <T> void createTable(DatabaseTableConfig<T> tableConfig, boolean dropAtEnd) throws Exception {
		try {
			// first we drop it in case it existed before
			dropTable(tableConfig, true);
		} catch (SQLException ignored) {
			// ignore any errors about missing tables
		}
		TableUtils.createTable(connectionSource, tableConfig);
		if (dropAtEnd) {
			dropTableConfigSet.add(tableConfig);
		}
	}

	protected <T> void dropTable(Class<T> clazz, boolean ignoreErrors) throws Exception {
		// drop the table and ignore any errors along the way
		TableUtils.dropTable(connectionSource, clazz, ignoreErrors);
	}

	protected <T> void dropTable(DatabaseTableConfig<T> tableConfig, boolean ignoreErrors) throws Exception {
		// drop the table and ignore any errors along the way
		TableUtils.dropTable(connectionSource, tableConfig, ignoreErrors);
	}

	protected static void removeDirectory(String path) {
		File file = new File(path);
		if (file.isDirectory()) {
			removeDirectory(file);
		} else {
			file.delete();
		}
	}

	protected static void removeDirectory(File dir) {
		for (File file : dir.listFiles()) {
			if (file.isDirectory()) {
				removeDirectory(file);
			} else {
				file.delete();
			}
		}
		dir.delete();
	}

	private void doOpenConnectionSource() throws Exception {
		if (connectionSource == null) {
			isConnectionExpected = isConnectionExpected();
			if (isConnectionExpected) {
				connectionSource = new JdbcConnectionSource(databaseUrl, userName, password);
			}
		}
		if (databaseType == null) {
			if (connectionSource != null) {
				databaseType = connectionSource.getDatabaseType();
			}
		} else {
			if (connectionSource != null) {
				connectionSource.setDatabaseType(databaseType);
			}
		}
	}

	private <T, ID> Dao<T, ID> configDao(BaseDaoImpl<T, ID> dao, boolean createTable) throws Exception {
		if (connectionSource == null) {
			throw new SQLException(DATASOURCE_ERROR);
		}
		dao.setConnectionSource(connectionSource);
		if (createTable) {
			DatabaseTableConfig<T> tableConfig = dao.getTableConfig();
			if (tableConfig == null) {
				tableConfig = DatabaseTableConfig.fromClass(databaseType, dao.getDataClass());
			}
			createTable(tableConfig, true);
		}
		dao.initialize();
		return dao;
	}
}
