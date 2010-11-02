package com.j256.ormlite.misc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;

import com.j256.ormlite.BaseOrmLiteJdbcTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.jdbc.JdbcPooledConnectionSource;

public class JdbcTransactionManagerTest extends BaseOrmLiteJdbcTest {

	@Override
	@Before
	public void before() throws Exception {
		if (databaseType != null) {
			return;
		}
		super.before();
		if (connectionSource != null) {
			connectionSource = new JdbcPooledConnectionSource(databaseUrl, userName, password, databaseType);
		}
	}

	@Test
	public void testDaoTransactionManagerCommitted() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TransactionManager mgr = new TransactionManager(connectionSource);
		testTransactionManager(mgr, null);
	}

	@Test
	public void testRollBack() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TransactionManager mgr = new TransactionManager(connectionSource);
		testTransactionManager(mgr, new RuntimeException("What!!  I protest!!"));
	}

	@Test
	public void testSpringWiredRollBack() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TransactionManager mgr = new TransactionManager();
		mgr.setConnectionSource(connectionSource);
		mgr.initialize();
		testTransactionManager(mgr, new RuntimeException("What!!  I protest!!"));
	}

	@Test
	public void testNonRuntimeExceptionWiredRollBack() throws Exception {
		if (connectionSource == null) {
			return;
		}
		TransactionManager mgr = new TransactionManager();
		mgr.setConnectionSource(connectionSource);
		mgr.initialize();
		testTransactionManager(mgr, new Exception("What!!  I protest via an Exception!!"));
	}

	@Test
	public void testTransactionWithinTransaction() throws Exception {
		if (connectionSource == null) {
			return;
		}
		final TransactionManager mgr = new TransactionManager(connectionSource);
		mgr.callInTransaction(new Callable<Void>() {
			public Void call() throws Exception {
				testTransactionManager(mgr, null);
				return null;
			}
		});
	}

	private void testTransactionManager(TransactionManager mgr, final Exception exception) throws Exception {
		final Dao<Foo, Integer> fooDao = createDao(Foo.class, true);
		final Foo foo1 = new Foo();
		String stuff = "stuff";
		foo1.stuff = stuff;
		assertEquals(1, fooDao.create(foo1));
		try {
			final int val = 13431231;
			int returned = mgr.callInTransaction(new Callable<Integer>() {
				public Integer call() throws Exception {
					// we delete it inside a transaction
					assertEquals(1, fooDao.delete(foo1));
					// we can't find it
					assertNull(fooDao.queryForId(foo1.id));
					if (exception != null) {
						// but then we throw an exception which rolls back the transaction
						throw exception;
					} else {
						return val;
					}
				}
			});
			if (exception == null) {
				assertEquals(val, returned);
			} else {
				fail("Should have thrown");
			}
		} catch (SQLException e) {
			if (exception == null) {
				throw e;
			} else {
				// expected
			}
		}

		if (exception == null) {
			// still doesn't find it after we delete it
			assertNull(fooDao.queryForId(foo1.id));
		} else {
			// still finds it after we delete it
			Foo foo2 = fooDao.queryForId(foo1.id);
			assertNotNull(foo2);
			assertEquals(stuff, foo2.stuff);
		}
	}

	public static class Foo {
		@DatabaseField(generatedId = true)
		int id;
		@DatabaseField
		String stuff;
		Foo() {
			// for ormlite
		}
	}
}
