package com.j256.ormlite.examples.common;

import java.sql.SQLException;

import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.support.ConnectionSource;

/**
 * Implementation of the Account DAO which is used to read/write Account to/from the database.
 */
public class AccountDaoImpl extends BaseDaoImpl<Account, Integer> implements AccountDao {

	/**
	 * Used by Spring which injects the DatabaseType afterwards.  If you are using Spring then your should
	 * use: init-method="initialize"
	 */
	public AccountDaoImpl() throws SQLException {
		super(Account.class);
	}

	public AccountDaoImpl(ConnectionSource connectionSource) throws SQLException {
		super(connectionSource, Account.class);
	}

	// no additional methods necessary unless you have per-Account specific DAO methods here
}
