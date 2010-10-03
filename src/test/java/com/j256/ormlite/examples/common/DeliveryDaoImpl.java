package com.j256.ormlite.examples.common;

import java.sql.SQLException;

import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTableConfig;

/**
 * Implementation of the Account DAO which is used to read/write Account to/from the database.
 */
public class DeliveryDaoImpl extends BaseDaoImpl<Delivery, Integer> implements DeliveryDao {

	/**
	 * Used by Spring which injects the DatabaseType afterwards.  If you are using Spring then your should
	 * use: init-method="initialize"
	 */
	public DeliveryDaoImpl() throws SQLException {
		super(Delivery.class);
	}

	public DeliveryDaoImpl(ConnectionSource connectionSource, DatabaseTableConfig<Delivery> tableConfig)
			throws SQLException {
		super(connectionSource, tableConfig);
	}

	// no additional methods necessary unless you have per-Account specific DAO methods here
}
