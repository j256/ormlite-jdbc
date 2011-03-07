package com.j256.ormlite.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.j256.ormlite.BaseJdbcTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.TableUtils;

public class TableCreatorTest extends BaseJdbcTest {

	@Test
	public void testInitialize() throws Exception {
		Dao<Foo, Object> fooDao = createDao(Foo.class, false);
		try {
			fooDao.create(new Foo());
			fail("Should have thrown an exception");
		} catch (SQLException e) {
			// expected
		}

		TableCreator tableCreator = new TableCreator();
		tableCreator.setConnectionSource(connectionSource);

		List<Dao<?, ?>> daoList = new ArrayList<Dao<?, ?>>();
		daoList.add(fooDao);
		tableCreator.setConfiguredDaos(daoList);
		try {
			System.setProperty(TableCreator.AUTO_CREATE_TABLES, Boolean.TRUE.toString());
			tableCreator.initialize();
		} finally {
			System.clearProperty(TableCreator.AUTO_CREATE_TABLES);
		}

		assertEquals(1, fooDao.create(new Foo()));
		// shouldn't do anything
		tableCreator.destroy();
		assertEquals(1, fooDao.create(new Foo()));

		try {
			System.setProperty(TableCreator.AUTO_DROP_TABLES, Boolean.TRUE.toString());
			tableCreator.destroy();
			fooDao.create(new Foo());
			fail("Should have thrown an exception");
		} catch (SQLException e) {
			// expected
		} finally {
			System.clearProperty(TableCreator.AUTO_DROP_TABLES);
		}
	}

	@Test
	public void testAutoCreateNotSet() throws Exception {
		TableCreator tableCreator = new TableCreator();
		tableCreator.initialize();
	}

	@Test(expected = SQLException.class)
	public void testNoConfiguredDaos() throws Exception {
		TableCreator tableCreator = new TableCreator();
		tableCreator.setConnectionSource(connectionSource);

		try {
			System.setProperty(TableCreator.AUTO_CREATE_TABLES, Boolean.TRUE.toString());
			tableCreator.initialize();
			fail("should not get here");
		} finally {
			System.clearProperty(TableCreator.AUTO_CREATE_TABLES);
		}
	}

	@Test
	public void testNonSpring() throws Exception {
		Dao<Foo, Object> fooDao = createDao(Foo.class, false);
		List<Dao<?, ?>> daoList = new ArrayList<Dao<?, ?>>();
		daoList.add(fooDao);
		TableCreator tableCreator = new TableCreator(connectionSource, daoList);
		try {
			System.setProperty(TableCreator.AUTO_CREATE_TABLES, Boolean.TRUE.toString());
			System.setProperty(TableCreator.AUTO_DROP_TABLES, Boolean.TRUE.toString());
			tableCreator.maybeCreateTables();
			tableCreator.maybeDropTables();
		} finally {
			System.clearProperty(TableCreator.AUTO_CREATE_TABLES);
			System.clearProperty(TableCreator.AUTO_DROP_TABLES);
		}
	}

	@Test
	public void testCreateAlreadyExists() throws Exception {
		Dao<Foo, Object> fooDao = createDao(Foo.class, true);
		List<Dao<?, ?>> daoList = new ArrayList<Dao<?, ?>>();
		daoList.add(fooDao);
		TableCreator tableCreator = new TableCreator(connectionSource, daoList);
		try {
			System.setProperty(TableCreator.AUTO_CREATE_TABLES, Boolean.TRUE.toString());
			tableCreator.maybeCreateTables();
		} finally {
			System.clearProperty(TableCreator.AUTO_CREATE_TABLES);
		}
	}

	@Test
	public void testDestroyDoesntExist() throws Exception {
		Dao<Foo, Object> fooDao = createDao(Foo.class, false);
		List<Dao<?, ?>> daoList = new ArrayList<Dao<?, ?>>();
		daoList.add(fooDao);
		TableCreator tableCreator = new TableCreator(connectionSource, daoList);
		try {
			System.setProperty(TableCreator.AUTO_CREATE_TABLES, Boolean.TRUE.toString());
			System.setProperty(TableCreator.AUTO_DROP_TABLES, Boolean.TRUE.toString());
			tableCreator.maybeCreateTables();
			TableUtils.dropTable(connectionSource, Foo.class, true);
			tableCreator.maybeDropTables();
		} finally {
			System.clearProperty(TableCreator.AUTO_CREATE_TABLES);
			System.clearProperty(TableCreator.AUTO_DROP_TABLES);
		}
	}

	protected static class Foo {
		@DatabaseField(generatedId = true)
		int id;
		@DatabaseField
		String stuff;
	}
}
