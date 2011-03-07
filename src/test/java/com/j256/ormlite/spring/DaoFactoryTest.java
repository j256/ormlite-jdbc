package com.j256.ormlite.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.j256.ormlite.BaseCoreTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.table.DatabaseTableConfig;

public class DaoFactoryTest extends BaseCoreTest {

	@Test
	public void testCreateDaoConnectionSourceClassOfT() throws Exception {
		createTable(Foo.class, true);
		Dao<Foo, Object> fooDao = DaoFactory.createDao(connectionSource, Foo.class);
		Foo foo = new Foo();
		String id = "this is the id";
		foo.id = id;
		assertEquals(1, fooDao.create(foo));
		Foo foo2 = fooDao.queryForId(foo.id);
		assertNotNull(foo2);
		assertEquals(id, foo2.id);
	}

	@Test
	public void testCreateDaoConnectionSourceDatabaseTableConfigOfT() throws Exception {
		createTable(Foo.class, true);
		Dao<Foo, Object> fooDao =
				DaoFactory.createDao(connectionSource, DatabaseTableConfig.fromClass(connectionSource, Foo.class));
		Foo foo = new Foo();
		String id = "this is the id";
		foo.id = id;
		assertEquals(1, fooDao.create(foo));
		Foo foo2 = fooDao.queryForId(foo.id);
		assertNotNull(foo2);
		assertEquals(id, foo2.id);
	}
}
