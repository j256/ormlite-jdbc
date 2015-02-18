package com.j256.ormlite.spring;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTableConfig;
import com.j256.ormlite.table.TableUtils;

/**
 * Spring bean that auto-creates any tables that it finds DAOs for if the property name in
 * TableCreator.AUTO_CREATE_TABLES property has been set to true. It will also auto-drop any tables that were
 * auto-created if the property name in TableCreator.AUTO_DROP_TABLES property has been set to true.
 * 
 * <p>
 * <b> NOTE: </b> If you are using the Spring type wiring in Java, {@link #initialize} should be called after all of the
 * set methods. In Spring XML, init-method="initialize" should be used.
 * </p>
 * 
 * <p>
 * Here is an example of spring wiring.
 * </p>
 * 
 * <pre>
 * &lt;!-- our database type factory-bean --&gt;
 * &lt;bean id="tableCreator" class="com.j256.ormlite.spring.TableCreator" init-method="initialize"&gt;
 * 	&lt;property name="connectionSource" ref="connectionSource" /&gt;
 * 	&lt;property name="configuredDaos"&gt;
 * 		&lt;list&gt;
 * 			&lt;ref bean="accountDao" /&gt;
 * 			&lt;ref bean="orderDao" /&gt;
 * 		&lt;/list&gt;
 * 	&lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 * 
 * @author graywatson
 */
public class TableCreator {

	public final static String AUTO_CREATE_TABLES = "ormlite.auto.create.tables";
	public final static String AUTO_DROP_TABLES = "ormlite.auto.drop.tables";

	private ConnectionSource connectionSource;
	private List<Dao<?, ?>> configuredDaos;
	private Set<DatabaseTableConfig<?>> createdClasses = new HashSet<DatabaseTableConfig<?>>();

	public TableCreator() {
		// for spring
	}

	public TableCreator(ConnectionSource connectionSource, List<Dao<?, ?>> configuredDaos) {
		this.connectionSource = connectionSource;
		this.configuredDaos = configuredDaos;
	}

	/**
	 * Possibly create the tables is the {@link #AUTO_CREATE_TABLES} system property is set to "true".
	 */
	public void maybeCreateTables() throws SQLException {
		initialize();
	}

	/**
	 * If you are using the Spring type wiring, this should be called after all of the set methods.
	 */
	public void initialize() throws SQLException {
		if (!Boolean.parseBoolean(System.getProperty(AUTO_CREATE_TABLES))) {
			return;
		}

		if (configuredDaos == null) {
			throw new SQLException("configuredDaos was not set in " + getClass().getSimpleName());
		}

		// find all of the daos and create the tables
		for (Dao<?, ?> dao : configuredDaos) {
			Class<?> clazz = dao.getDataClass();
			try {
				DatabaseTableConfig<?> tableConfig = null;
				if (dao instanceof BaseDaoImpl) {
					tableConfig = ((BaseDaoImpl<?, ?>) dao).getTableConfig();
				}
				if (tableConfig == null) {
					tableConfig = DatabaseTableConfig.fromClass(connectionSource, clazz);
				}
				TableUtils.createTable(connectionSource, tableConfig);
				createdClasses.add(tableConfig);
			} catch (Exception e) {
				// we don't stop because the table might already exist
				System.err.println("Was unable to auto-create table for " + clazz);
				e.printStackTrace();
			}
		}
	}

	/**
	 * Possibly drop the tables that were previously created if the {@link #AUTO_DROP_TABLES} system property is set to
	 * "true".
	 */
	public void maybeDropTables() {
		destroy();
	}

	public void destroy() {
		if (!Boolean.parseBoolean(System.getProperty(AUTO_DROP_TABLES))) {
			return;
		}
		for (DatabaseTableConfig<?> tableConfig : createdClasses) {
			try {
				TableUtils.dropTable(connectionSource, tableConfig, false);
			} catch (Exception e) {
				// we don't stop because the table might already exist
				System.err.println("Was unable to auto-drop table for " + tableConfig.getDataClass());
				e.printStackTrace();
			}
		}
		createdClasses.clear();
	}

	// This is @Required
	public void setConnectionSource(ConnectionSource dataSource) {
		this.connectionSource = dataSource;
	}

	public void setConfiguredDaos(List<Dao<?, ?>> configuredDaos) {
		this.configuredDaos = configuredDaos;
	}
}
