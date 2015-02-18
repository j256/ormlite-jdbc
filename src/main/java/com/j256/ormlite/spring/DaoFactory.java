package com.j256.ormlite.spring;

import java.sql.SQLException;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTableConfig;

/**
 * <p>
 * Spring bean that can be used to create Dao's of certain classes without needing their own Dao class.
 * </p>
 * 
 * <p>
 * Here is an example of spring wiring. See the Spring example in the documentation for more info.
 * </p>
 * 
 * <pre>
 * 	&lt;bean id="accountDao" class="com.j256.ormlite.spring.DaoFactory" factory-method="createDao"&gt;
 * 		&lt;constructor-arg index="0" ref="connectionSource" /&gt;
 * 		&lt;constructor-arg index="1" value="com.j256.ormlite.examples.spring.Account" /&gt;
 * 	&lt;/bean&gt;
 * </pre>
 * 
 * @author graywatson
 */
public class DaoFactory {

	/**
	 * Create and return a Dao based on the arguments.
	 */
	public static <T, ID> Dao<T, ID> createDao(ConnectionSource connectionSource, Class<T> clazz) throws SQLException {
		return DaoManager.createDao(connectionSource, clazz);
	}

	/**
	 * Create and return a Dao based on the arguments.
	 */
	public static <T, ID> Dao<T, ID> createDao(ConnectionSource connectionSource, DatabaseTableConfig<T> tableConfig)
			throws SQLException {
		return DaoManager.createDao(connectionSource, tableConfig);
	}
}
