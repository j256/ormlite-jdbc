package com.j256.ormlite;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.jdbc.JdbcDatabaseConnection;
import com.j256.ormlite.support.DatabaseConnection;

/**
 * Wrapped connection source for JDBC testing purposes.
 * 
 * @author graywatson
 */
public class WrappedJdbcConnectionSource extends WrappedConnectionSource {

	public WrappedJdbcConnectionSource(JdbcConnectionSource cs) {
		super(cs);
	}

	@Override
	protected WrappedConnection wrapConnection(DatabaseConnection connection) {
		JdbcDatabaseConnection conn = (JdbcDatabaseConnection) connection;
		WrappedJdbcConnection wrapped = new WrappedJdbcConnection(connection, conn.getInternalConnection());
		conn.setInternalConnection(wrapped.getConnectionProxy());
		return wrapped;
	}

	private static class WrappedJdbcConnection implements WrappedConnection, InvocationHandler {

		private final DatabaseConnection databaseConnection;
		private final Connection connection;
		private final Connection connectionProxy;
		private List<WrappedPreparedStatement> wrappedStatements = new ArrayList<WrappedPreparedStatement>();

		public WrappedJdbcConnection(DatabaseConnection databaseConnection, Connection connection) {
			this.databaseConnection = databaseConnection;
			this.connection = connection;
			this.connectionProxy = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
					new Class<?>[] { Connection.class }, this);
		}

		@Override
		public DatabaseConnection getDatabaseConnectionProxy() {
			return databaseConnection;
		}

		public Connection getConnectionProxy() {
			return connectionProxy;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			// System.err.println("Running method on Connection." + method.getName());
			try {
				Object obj = method.invoke(connection, args);
				if (method.getName().equals("prepareStatement") && obj instanceof PreparedStatement) {
					WrappedPreparedStatement wrappedStatement = new WrappedPreparedStatement((PreparedStatement) obj);
					wrappedStatements.add(wrappedStatement);
					obj = wrappedStatement.getPreparedStatement();
				}
				return obj;
			} catch (InvocationTargetException e) {
				// pass on the exception
				throw e.getTargetException();
			}
		}

		@Override
		public boolean isOkay() {
			for (WrappedPreparedStatement wrappedStatement : wrappedStatements) {
				if (!wrappedStatement.isOkay()) {
					return false;
				}
			}
			return true;
		}

		@Override
		public void close() {
			wrappedStatements.clear();
		}
	}

	private static class WrappedPreparedStatement implements InvocationHandler {

		private final Object statementProxy;
		private final PreparedStatement preparedStatment;
		private boolean closeCalled = false;

		public WrappedPreparedStatement(PreparedStatement preparedStatment) {
			this.preparedStatment = preparedStatment;
			this.statementProxy = Proxy.newProxyInstance(getClass().getClassLoader(),
					new Class<?>[] { PreparedStatement.class }, this);
		}

		public PreparedStatement getPreparedStatement() {
			return (PreparedStatement) statementProxy;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			// System.err.println("Running method on PreparedStatement." + method.getName());
			try {
				Object obj = method.invoke(preparedStatment, args);
				if (method.getName().equals("close")) {
					closeCalled = true;
				}
				return obj;
			} catch (InvocationTargetException e) {
				// pass on the exception
				throw e.getTargetException();
			}
		}

		public boolean isOkay() {
			if (closeCalled) {
				return true;
			} else {
				System.err.println("PreparedStatement was not closed: " + preparedStatment.toString());
				return false;
			}
		}
	}
}
