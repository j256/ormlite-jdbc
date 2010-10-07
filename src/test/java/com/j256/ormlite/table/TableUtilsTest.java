package com.j256.ormlite.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.Test;

import com.j256.ormlite.BaseOrmLiteJdbcTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;

public class TableUtilsTest extends BaseOrmLiteJdbcTest {

	@Test
	public void testCreateTable() throws Exception {
		Dao<Foo, String> fooDao = createDao(Foo.class, false);
		// first we create the table
		createTable(Foo.class, false);
		// test it out
		assertEquals(0, fooDao.queryForAll().size());
		// now we drop it
		dropTable(Foo.class, true);
		try {
			// query should fail
			fooDao.queryForAll();
			fail("Was expecting a SQL exception");
		} catch (Exception expected) {
			// expected
		}
		// now create it again
		createTable(Foo.class, false);
		assertEquals(0, fooDao.queryForAll().size());
		dropTable(Foo.class, true);
	}

	@Test(expected = SQLException.class)
	public void testDoubleDrop() throws Exception {
		Dao<Foo, String> fooDao = createDao(Foo.class, false);
		// first we create the table
		createTable(Foo.class, false);
		// test it out
		assertEquals(0, fooDao.queryForAll().size());
		// now we drop it
		dropTable(Foo.class, true);
		// this should fail
		dropTable(Foo.class, false);
	}

	protected static class Foo {
		@DatabaseField
		String name;
	}
}
