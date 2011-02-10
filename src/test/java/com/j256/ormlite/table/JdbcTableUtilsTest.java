package com.j256.ormlite.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.Test;

import com.j256.ormlite.BaseJdbcTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;

public class JdbcTableUtilsTest extends BaseJdbcTest {

	@Test(expected = SQLException.class)
	public void testMissingCreate() throws Exception {
		Dao<Foo, Integer> fooDao = createDao(Foo.class, false);
		fooDao.queryForAll();
	}

	@Test
	public void testCreateTable() throws Exception {
		Dao<Foo, Integer> fooDao = createDao(Foo.class, false);
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
	public void testDropThenQuery() throws Exception {
		Dao<Foo, Integer> fooDao = createDao(Foo.class, true);
		assertEquals(0, fooDao.queryForAll().size());
		dropTable(Foo.class, true);
		fooDao.queryForAll();
	}

	@Test(expected = SQLException.class)
	public void testRawExecuteDropThenQuery() throws Exception {
		Dao<Foo, Integer> fooDao = createDao(Foo.class, true);
		StringBuilder sb = new StringBuilder();
		sb.append("DROP TABLE ");
		if (databaseType.isEntityNamesMustBeUpCase()) {
			databaseType.appendEscapedEntityName(sb, "FOO");
		} else {
			databaseType.appendEscapedEntityName(sb, "foo");
		}
		assertEquals(0, fooDao.executeRaw(sb.toString()));
		fooDao.queryForAll();
	}

	@Test(expected = SQLException.class)
	public void testDoubleDrop() throws Exception {
		Dao<Foo, Integer> fooDao = createDao(Foo.class, false);
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
		@DatabaseField(generatedId = true)
		int id;
		@DatabaseField
		String name;
	}
}
