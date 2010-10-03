package com.j256.ormlite.examples.common;

import java.sql.SQLException;

import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.support.ConnectionSource;

/**
 * Implementation of the Order DAO which is used to read/write Order to/from the database.
 */
public class OrderDaoImpl extends BaseDaoImpl<Order, Integer> implements OrderDao {

	/**
	 * Used by Spring which injects the DatabaseType afterwards.  If you are using Spring then your should
	 * use: init-method="initialize"
	 */
	public OrderDaoImpl() throws SQLException {
		super(Order.class);
	}

	public OrderDaoImpl(ConnectionSource connectionSource) throws SQLException {
		super(connectionSource, Order.class);
	}

	// no additional methods necessary unless you have per-Order specific DAO methods here
}
