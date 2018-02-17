package com.j256.ormlite.stmt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.j256.ormlite.BaseJdbcTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;

public class JdbcQueryBuilderTest extends BaseJdbcTest {

	private final static String ID_PREFIX = "id";
	private final static int LOW_VAL = 21114;
	private final static int HIGH_VAL = LOW_VAL + 499494;
	private final static int EQUAL_VAL = 21312312;
	private Foo foo1;
	private Foo foo2;

	@Test
	@SuppressWarnings("unchecked")
	public void testAnd() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		// test And + Eq
		qb.where().eq(Foo.ID_COLUMN_NAME, foo1.id).and().eq(Foo.VAL_COLUMN_NAME, foo1.val);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));

		// test And + Eq not inline
		Where<Foo, String> where = qb.where();
		where.eq(Foo.ID_COLUMN_NAME, foo2.id);
		where.and();
		where.eq(Foo.VAL_COLUMN_NAME, foo2.val);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));

		// test And double args
		where = qb.where();
		where.and(where.eq(Foo.ID_COLUMN_NAME, foo1.id), where.eq(Foo.VAL_COLUMN_NAME, foo1.val));
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testOr() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		// test Or + Eq
		qb.where().eq(Foo.ID_COLUMN_NAME, foo1.id).or().eq(Foo.VAL_COLUMN_NAME, foo1.val);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));

		// test Or + Eq not inline
		Where<Foo, String> where = qb.where();
		where.eq(Foo.ID_COLUMN_NAME, foo2.id);
		where.or();
		where.eq(Foo.VAL_COLUMN_NAME, foo2.val);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));

		// test Or of ands
		where = qb.where();
		where.or(where.and(where.eq(Foo.ID_COLUMN_NAME, foo1.id), where.eq(Foo.VAL_COLUMN_NAME, foo1.val)),
				where.eq(Foo.ID_COLUMN_NAME, foo2.id).and().eq(Foo.VAL_COLUMN_NAME, foo2.val));
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testSelectArgs() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		SelectArg idSelectArg = new SelectArg();
		qb.where().eq(Foo.ID_COLUMN_NAME, idSelectArg);
		PreparedQuery<Foo> preparedQuery = qb.prepare();

		idSelectArg.setValue(foo1.id);
		List<Foo> results = fooDao.query(preparedQuery);
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));

		idSelectArg.setValue(foo2.id);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));
	}

	@Test
	public void testLike() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().like(Foo.ID_COLUMN_NAME, ID_PREFIX + "%");
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testSelectArgsNotSet() throws Exception {

		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		SelectArg idSelectArg = new SelectArg();
		qb.where().eq(Foo.ID_COLUMN_NAME, idSelectArg);
		try {
			fooDao.query(qb.prepare());
			fail("expected exception");
		} catch (SQLException e) {
			// expected
			System.err.println("Expected: " + e);
			e.printStackTrace();
		}
	}

	@Test
	public void testSelectNot() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().not().eq(Foo.ID_COLUMN_NAME, foo1.id);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));
	}

	@Test
	public void testIn() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().in(Foo.ID_COLUMN_NAME, foo1.id, foo2.id);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testInIterable() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().in(Foo.ID_COLUMN_NAME, Arrays.asList(foo1.id, foo2.id));
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testExists() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> innerQb = fooDao.queryBuilder();
		innerQb.where().idEq(foo1.id);
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().exists(innerQb);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testExistsNoEntries() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> innerQb = fooDao.queryBuilder();
		innerQb.where().idEq("no id by this name");
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().exists(innerQb);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());
	}

	@Test
	public void testNotExists() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> innerQb = fooDao.queryBuilder();
		innerQb.where().idEq(foo1.id);
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().not().exists(innerQb);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());
	}

	@Test
	public void testNotIn() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().not().in(Foo.ID_COLUMN_NAME, foo1.id);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));
	}

	@Test
	public void testNotBad() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		qb.where().not();
		try {
			fooDao.query(qb.prepare());
			fail("expected exception");
		} catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testNotNotComparison() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		Where<Foo, String> where = qb.where();
		try {
			where.not(where.and(where.eq(Foo.ID_COLUMN_NAME, foo1.id), where.eq(Foo.ID_COLUMN_NAME, foo1.id)));
			fail("expected exception");
		} catch (IllegalArgumentException e) {
			// expected
		}
	}

	@Test
	public void testNotArg() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		Where<Foo, String> where = qb.where();
		where.not(where.eq(Foo.ID_COLUMN_NAME, foo1.id));
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));
	}

	@Test
	public void testNoWhereOperations() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		qb.where();
		try {
			fooDao.query(qb.prepare());
			fail("expected exception");
		} catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testMissingAnd() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		qb.where().eq(Foo.ID_COLUMN_NAME, foo1.id).eq(Foo.ID_COLUMN_NAME, foo1.id);
		try {
			fooDao.query(qb.prepare());
			fail("expected exception");
		} catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testMissingAndArg() throws Exception {
		Dao<Foo, String> fooDao = createDao(Foo.class, false);
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		try {
			qb.where().and();
			fail("expected exception");
		} catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testBetween() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().between(Foo.VAL_COLUMN_NAME, LOW_VAL, HIGH_VAL);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));

		qb.where().between(Foo.VAL_COLUMN_NAME, LOW_VAL + 1, HIGH_VAL);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));

		qb.where().between(Foo.VAL_COLUMN_NAME, LOW_VAL, HIGH_VAL - 1);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));

		qb.where().between(Foo.VAL_COLUMN_NAME, LOW_VAL + 1, HIGH_VAL - 1);
		results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());
	}

	@Test
	public void testBetweenSelectArg() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		SelectArg lowSelectArg = new SelectArg();
		qb.where().between(Foo.VAL_COLUMN_NAME, lowSelectArg, HIGH_VAL);
		lowSelectArg.setValue(LOW_VAL);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));

		SelectArg highSelectArg = new SelectArg();
		lowSelectArg.setValue(LOW_VAL + 1);
		highSelectArg.setValue(HIGH_VAL);
		qb.where().between(Foo.VAL_COLUMN_NAME, lowSelectArg, highSelectArg);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));
	}

	@Test
	public void testBetweenStrings() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		String low = ID_PREFIX;
		String high = ID_PREFIX + "99999";
		qb.where().between(Foo.ID_COLUMN_NAME, low, high);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testLtGtEtc() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		qb.where().eq(Foo.VAL_COLUMN_NAME, foo1.val);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));

		qb.where().ge(Foo.VAL_COLUMN_NAME, foo1.val);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));

		qb.where().ge(Foo.VAL_COLUMN_NAME, foo1.val - 1);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));

		qb.where().ge(Foo.VAL_COLUMN_NAME, foo2.val);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));

		qb.where().gt(Foo.VAL_COLUMN_NAME, foo1.val);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));

		qb.where().gt(Foo.VAL_COLUMN_NAME, foo1.val - 1);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));

		qb.where().gt(Foo.VAL_COLUMN_NAME, foo2.val);
		results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());

		qb.where().le(Foo.VAL_COLUMN_NAME, foo1.val);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));

		qb.where().le(Foo.VAL_COLUMN_NAME, foo1.val - 1);
		results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());

		qb.where().lt(Foo.VAL_COLUMN_NAME, foo1.val);
		results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());

		qb.where().lt(Foo.VAL_COLUMN_NAME, foo1.val + 1);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));

		qb.where().ne(Foo.VAL_COLUMN_NAME, foo1.val);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo2, results.get(0));

		qb.where().ne(Foo.VAL_COLUMN_NAME, foo1.val).and().ne(Foo.VAL_COLUMN_NAME, foo2.val);
		results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());
	}

	@Test
	public void testPartialIteratorRetrieval() throws Exception {
		Dao<PartialData, Integer> partialDao = createDao(PartialData.class, true);
		List<String> firsts = new ArrayList<String>();
		List<String> lasts = new ArrayList<String>();
		List<Integer> ids = new ArrayList<Integer>();

		createPartial(partialDao, ids, firsts, lasts, "bill", "rambo");
		createPartial(partialDao, ids, firsts, lasts, "zippy", "dingo");
		createPartial(partialDao, ids, firsts, lasts, "crappy", "bladdero");
		checkPartialList(partialDao.queryForAll(), ids, firsts, lasts, false, false);
		checkPartialIterator(partialDao.iterator(), ids, firsts, lasts, false, false);

		Set<String> columnNames = new HashSet<String>();
		columnNames.add(PartialData.ID_FIELD_NAME);
		QueryBuilder<PartialData, Integer> qb = partialDao.queryBuilder();
		qb.selectColumns(columnNames);
		Iterator<PartialData> iterator = partialDao.iterator(qb.prepare());
		checkPartialIterator(iterator, ids, firsts, lasts, true, true);

		columnNames = new HashSet<String>();
		columnNames.add(PartialData.FIRST_FIELD_NAME);
		qb.selectColumns(columnNames);
		iterator = partialDao.iterator(qb.prepare());
		checkPartialIterator(iterator, ids, firsts, lasts, false, true);

		columnNames = new HashSet<String>();
		columnNames.add(PartialData.LAST_FIELD_NAME);
		qb.selectColumns(columnNames);
		iterator = partialDao.iterator(qb.prepare());
		checkPartialIterator(iterator, ids, firsts, lasts, false, false);

		for (PartialData partialData : partialDao) {
			assertEquals(1, partialDao.delete(partialData));
		}
		assertEquals(0, partialDao.queryForAll().size());
	}

	@Test
	public void testIteratorCustomQuery() throws Exception {
		Dao<PartialData, Integer> partialDao = createDao(PartialData.class, true);
		List<String> firsts = new ArrayList<String>();
		List<String> lasts = new ArrayList<String>();
		List<Integer> ids = new ArrayList<Integer>();

		String firstFirst = "bill";
		createPartial(partialDao, ids, firsts, lasts, firstFirst, "rambo");
		createPartial(partialDao, ids, firsts, lasts, "zippy", "dingo");
		createPartial(partialDao, ids, firsts, lasts, "crappy", "bladdero");
		checkPartialList(partialDao.queryForAll(), ids, firsts, lasts, false, false);
		checkPartialIterator(partialDao.iterator(), ids, firsts, lasts, false, false);

		QueryBuilder<PartialData, Integer> qb = partialDao.queryBuilder();
		qb.where().eq(PartialData.FIRST_FIELD_NAME, firstFirst);
		Iterator<PartialData> iterator = partialDao.iterator(qb.prepare());
		assertTrue(iterator.hasNext());
		assertEquals(firstFirst, iterator.next().first);
		assertFalse(iterator.hasNext());

		SelectArg firstArg = new SelectArg();
		qb.where().eq(PartialData.FIRST_FIELD_NAME, firstArg);
		firstArg.setValue(firstFirst);
		iterator = partialDao.iterator(qb.prepare());
		assertTrue(iterator.hasNext());
		assertEquals(firstFirst, iterator.next().first);
		assertFalse(iterator.hasNext());
	}

	@Test
	public void testUnknownColumn() throws Exception {
		Dao<Foo, String> fooDao = createDao(Foo.class, false);
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		try {
			qb.selectColumns("unknown column");
			fail("expected exception");
		} catch (IllegalArgumentException e) {
			// expected
		}
	}

	@Test
	public void testOrderBy() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		qb.orderBy(Foo.VAL_COLUMN_NAME, true);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));

		qb = fooDao.queryBuilder();
		;
		qb.orderBy(Foo.VAL_COLUMN_NAME, false);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo2, results.get(0));
		assertEquals(foo1, results.get(1));

		// should be the same order
		qb = fooDao.queryBuilder();
		;
		qb.orderBy(Foo.EQUAL_COLUMN_NAME, false);
		qb.orderBy(Foo.VAL_COLUMN_NAME, false);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo2, results.get(0));
		assertEquals(foo1, results.get(1));
	}

	@Test
	public void testGroupBy() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		qb.selectColumns(Foo.EQUAL_COLUMN_NAME);
		qb.groupBy(Foo.EQUAL_COLUMN_NAME);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(EQUAL_VAL, results.get(0).equal);
		assertNull(results.get(0).id);
	}

	@Test
	public void testGroupAndOrderBy() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		qb.selectColumns(Foo.EQUAL_COLUMN_NAME, Foo.ID_COLUMN_NAME);
		qb.groupBy(Foo.EQUAL_COLUMN_NAME);
		qb.groupBy(Foo.ID_COLUMN_NAME);
		// get strange order otherwise
		qb.orderBy(Foo.ID_COLUMN_NAME, true);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testLimit() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		// no limit the default
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
		qb.limit(1L);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));
		// set back to no-limit
		qb.limit(null);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testLimitDoublePrepare() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		// no limit the default
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
		qb.limit(1L);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));
	}

	@Test
	public void testLimitAfterSelect() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		// no limit the default
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
		qb.limit(1L);
		results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));
		// set back to no-limit
		qb.limit(null);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testOffsetWithLimit() throws Exception {
		Dao<Foo, Object> dao = createDao(Foo.class, true);
		Foo foo1 = new Foo();
		foo1.id = "stuff1";
		assertEquals(1, dao.create(foo1));
		Foo foo2 = new Foo();
		foo2.id = "stuff2";
		assertEquals(1, dao.create(foo2));
		assertEquals(2, dao.queryForAll().size());

		QueryBuilder<Foo, Object> qb = dao.queryBuilder();
		long offset = 1;
		long limit = 2;
		qb.offset(offset);
		qb.limit(limit);
		List<Foo> results = dao.query(qb.prepare());

		assertEquals(1, results.size());
	}

	@Test
	public void testOffsetNoLimit() throws Exception {
		Dao<Foo, Object> dao = createDao(Foo.class, true);
		Foo foo1 = new Foo();
		foo1.id = "stuff1";
		assertEquals(1, dao.create(foo1));
		Foo foo2 = new Foo();
		foo2.id = "stuff2";
		assertEquals(1, dao.create(foo2));
		assertEquals(2, dao.queryForAll().size());

		QueryBuilder<Foo, Object> qb = dao.queryBuilder();
		qb.offset(1L);
		try {
			dao.query(qb.prepare());
			fail("expected exception");
		} catch (SQLException e) {
			// expected
		}
	}

	@Test
	public void testReturnId() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		qb.selectColumns(Foo.ID_COLUMN_NAME);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1.id, results.get(0).id);
		assertEquals(0, results.get(0).val);
		assertEquals(foo2.id, results.get(1).id);
		assertEquals(0, results.get(1).val);
	}

	@Test
	public void testDistinct() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		qb.distinct().selectColumns(Foo.EQUAL_COLUMN_NAME);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(1, results.size());
		assertEquals(EQUAL_VAL, results.get(0).equal);
		assertNull(results.get(0).id);
	}

	@Test
	public void testIsNull() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();

		// null fields start off as null so 0 are not-null
		qb.where().isNotNull(Foo.NULL_COLUMN_NAME);
		List<Foo> results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());

		// all are null
		qb.where().isNull(Foo.NULL_COLUMN_NAME);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));

		// set the null fields to not-null
		for (Foo foo : results) {
			foo.nullField = "not null";
			assertEquals(1, fooDao.update(foo));
		}

		// no null results should be found
		qb.where().isNull(Foo.NULL_COLUMN_NAME);
		results = fooDao.query(qb.prepare());
		assertEquals(0, results.size());

		// all are not-null
		qb.where().isNotNull(Foo.NULL_COLUMN_NAME);
		results = fooDao.query(qb.prepare());
		assertEquals(2, results.size());
		assertEquals(foo1, results.get(0));
		assertEquals(foo2, results.get(1));
	}

	@Test
	public void testSetWhere() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		Where<Foo, String> where = qb.where();
		where.eq(Foo.ID_COLUMN_NAME, foo1.id);
		List<Foo> list = fooDao.query(qb.prepare());
		assertEquals(1, list.size());
		assertEquals(foo1, list.get(0));

		qb = fooDao.queryBuilder();
		qb.setWhere(where);
		list = fooDao.query(qb.prepare());
		assertEquals(1, list.size());
		assertEquals(foo1, list.get(0));
	}

	@Test
	public void testQueryForStringInt() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		QueryBuilder<Foo, String> qb = fooDao.queryBuilder();
		Where<Foo, String> where = qb.where();
		// testing the val column with a integer as a string
		where.eq(Foo.VAL_COLUMN_NAME, Integer.toString(foo1.val));
		List<Foo> list = fooDao.query(qb.prepare());
		assertEquals(1, list.size());
		assertEquals(foo1, list.get(0));
	}

	@Test
	public void testWherePrepare() throws Exception {
		Dao<Foo, String> fooDao = createTestData();
		List<Foo> results = fooDao.query(fooDao.queryBuilder()
				.where()
				.eq(Foo.ID_COLUMN_NAME, foo1.id)
				.and()
				.eq(Foo.VAL_COLUMN_NAME, foo1.val)
				.prepare());
		assertEquals(1, results.size());
		assertEquals(foo1, results.get(0));
	}

	@Test
	public void testIdEq() throws Exception {
		Dao<Foo, Integer> fooDao = createDao(Foo.class, true);

		Foo foo = new Foo();
		foo.id = "wow id wow";
		assertEquals(1, fooDao.create(foo));

		List<Foo> results = fooDao.query(fooDao.queryBuilder().where().idEq(fooDao, foo).prepare());
		assertEquals(1, results.size());
		assertEquals(foo.id, results.get(0).id);
	}

	@Test
	public void testSimpleJoin() throws Exception {
		Dao<Bar, Integer> barDao = createDao(Bar.class, true);
		Dao<Baz, Integer> bazDao = createDao(Baz.class, true);

		Bar bar1 = new Bar();
		bar1.val = 2234;
		assertEquals(1, barDao.create(bar1));
		Bar bar2 = new Bar();
		bar2.val = 324322234;
		assertEquals(1, barDao.create(bar2));

		Baz baz1 = new Baz();
		baz1.bar = bar1;
		assertEquals(1, bazDao.create(baz1));
		Baz baz2 = new Baz();
		baz2.bar = bar2;
		assertEquals(1, bazDao.create(baz2));

		QueryBuilder<Bar, Integer> barQb = barDao.queryBuilder();
		barQb.where().eq(Bar.VAL_FIELD, bar1.val);
		List<Baz> results = bazDao.queryBuilder().query();
		assertEquals(2, results.size());
		results = bazDao.queryBuilder().join(barQb).query();
		assertEquals(1, results.size());
		assertEquals(bar1.id, results.get(0).bar.id);
	}

	@Test
	public void testReverseJoin() throws Exception {
		Dao<Bar, Integer> barDao = createDao(Bar.class, true);
		Dao<Baz, Integer> bazDao = createDao(Baz.class, true);

		Bar bar1 = new Bar();
		bar1.val = 2234;
		assertEquals(1, barDao.create(bar1));
		Bar bar2 = new Bar();
		bar2.val = 324322234;
		assertEquals(1, barDao.create(bar2));

		Baz baz1 = new Baz();
		baz1.bar = bar1;
		assertEquals(1, bazDao.create(baz1));
		Baz baz2 = new Baz();
		baz2.bar = bar2;
		assertEquals(1, bazDao.create(baz2));

		QueryBuilder<Baz, Integer> bazQb = bazDao.queryBuilder();
		bazQb.where().eq(Baz.ID_FIELD, baz1.id);
		List<Bar> results = barDao.queryBuilder().query();
		assertEquals(2, results.size());
		results = barDao.queryBuilder().join(bazQb).query();
		assertEquals(1, results.size());
		assertEquals(bar1.val, results.get(0).val);
	}

	@Test
	public void testJoinDoubleWhere() throws Exception {
		Dao<Bar, Integer> barDao = createDao(Bar.class, true);
		Dao<Baz, Integer> bazDao = createDao(Baz.class, true);

		Bar bar1 = new Bar();
		bar1.val = 2234;
		assertEquals(1, barDao.create(bar1));
		Bar bar2 = new Bar();
		bar2.val = 324322234;
		assertEquals(1, barDao.create(bar2));

		Baz baz1 = new Baz();
		baz1.bar = bar1;
		assertEquals(1, bazDao.create(baz1));
		Baz baz2 = new Baz();
		// both have bar1
		baz2.bar = bar1;
		assertEquals(1, bazDao.create(baz2));

		QueryBuilder<Bar, Integer> barQb = barDao.queryBuilder();
		barQb.where().eq(Bar.VAL_FIELD, bar1.val);
		QueryBuilder<Baz, Integer> bazQb = bazDao.queryBuilder();
		bazQb.where().eq(Baz.ID_FIELD, baz1.id);
		List<Baz> results = bazQb.join(barQb).query();
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(bar1.id, results.get(0).bar.id);
	}

	@Test
	public void testJoinOrder() throws Exception {
		Dao<Bar, Integer> barDao = createDao(Bar.class, true);
		Dao<Baz, Integer> bazDao = createDao(Baz.class, true);

		Bar bar1 = new Bar();
		bar1.val = 2234;
		assertEquals(1, barDao.create(bar1));
		Bar bar2 = new Bar();
		bar2.val = 324322234;
		assertEquals(1, barDao.create(bar2));

		Baz baz1 = new Baz();
		baz1.bar = bar1;
		assertEquals(1, bazDao.create(baz1));
		Baz baz2 = new Baz();
		baz2.bar = bar2;
		assertEquals(1, bazDao.create(baz2));

		QueryBuilder<Bar, Integer> barQb = barDao.queryBuilder();
		barQb.orderBy(Bar.VAL_FIELD, true);
		List<Baz> results = bazDao.queryBuilder().join(barQb).query();
		assertEquals(2, results.size());
		assertEquals(bar1.id, results.get(0).bar.id);
		assertEquals(bar2.id, results.get(1).bar.id);

		// reset the query to change the order direction
		barQb.reset();
		barQb.orderBy(Bar.VAL_FIELD, false);
		results = bazDao.queryBuilder().join(barQb).query();
		assertEquals(2, results.size());
		assertEquals(bar2.id, results.get(0).bar.id);
		assertEquals(bar1.id, results.get(1).bar.id);
	}

	@Test
	public void testJoinMultipleOrder() throws Exception {
		Dao<Bar, Integer> barDao = createDao(Bar.class, true);
		Dao<Baz, Integer> bazDao = createDao(Baz.class, true);

		Bar bar1 = new Bar();
		bar1.val = 2234;
		assertEquals(1, barDao.create(bar1));
		Bar bar2 = new Bar();
		bar2.val = 324322234;
		assertEquals(1, barDao.create(bar2));

		Baz baz1 = new Baz();
		baz1.bar = bar1;
		assertEquals(1, bazDao.create(baz1));
		Baz baz2 = new Baz();
		baz2.bar = bar2;
		assertEquals(1, bazDao.create(baz2));

		QueryBuilder<Bar, Integer> barQb = barDao.queryBuilder();
		barQb.where().eq(Bar.VAL_FIELD, bar1.val);
		barQb.orderBy(Bar.ID_FIELD, true);
		List<Baz> results = bazDao.queryBuilder().query();
		assertEquals(2, results.size());
		results = bazDao.queryBuilder().orderBy(Baz.ID_FIELD, true).join(barQb).query();
		assertEquals(1, results.size());
		assertEquals(bar1.id, results.get(0).bar.id);
	}

	@Test
	public void testLeftJoin() throws Exception {
		Dao<Bar, Integer> barDao = createDao(Bar.class, true);
		Dao<Baz, Integer> bazDao = createDao(Baz.class, true);

		Bar bar1 = new Bar();
		bar1.val = 2234;
		assertEquals(1, barDao.create(bar1));
		Bar bar2 = new Bar();
		bar2.val = 324322234;
		assertEquals(1, barDao.create(bar2));

		Baz baz1 = new Baz();
		baz1.bar = bar1;
		assertEquals(1, bazDao.create(baz1));
		Baz baz2 = new Baz();
		baz2.bar = bar2;
		assertEquals(1, bazDao.create(baz2));
		Baz baz3 = new Baz();
		// no bar
		assertEquals(1, bazDao.create(baz3));

		QueryBuilder<Bar, Integer> barQb = barDao.queryBuilder();
		List<Baz> results = bazDao.queryBuilder().query();
		assertEquals(3, results.size());
		results = bazDao.queryBuilder().join(barQb).query();
		assertEquals(2, results.size());
		results = bazDao.queryBuilder().leftJoin(barQb).query();
		assertEquals(3, results.size());
	}

	@Test
	public void testInnerJoin() throws Exception {
		Dao<Bar, Integer> barDao = createDao(Bar.class, true);
		Dao<Baz, Integer> bazDao = createDao(Baz.class, true);
		Dao<Bing, Integer> bingDao = createDao(Bing.class, true);

		Bar bar1 = new Bar();
		bar1.val = 2234;
		assertEquals(1, barDao.create(bar1));
		Bar bar2 = new Bar();
		bar2.val = 324322234;
		assertEquals(1, barDao.create(bar2));

		Baz baz1 = new Baz();
		baz1.bar = bar1;
		assertEquals(1, bazDao.create(baz1));
		Baz baz2 = new Baz();
		baz2.bar = bar2;
		assertEquals(1, bazDao.create(baz2));

		Bing bing1 = new Bing();
		bing1.baz = baz1;
		assertEquals(1, bingDao.create(bing1));
		Bing bing2 = new Bing();
		bing2.baz = baz2;
		assertEquals(1, bingDao.create(bing2));

		QueryBuilder<Bar, Integer> barQb = barDao.queryBuilder();
		barQb.where().eq(Bar.VAL_FIELD, bar1.val);

		QueryBuilder<Baz, Integer> bazQb = bazDao.queryBuilder();
		assertEquals(2, bazQb.query().size());
		bazQb.join(barQb);

		List<Bing> results = bingDao.queryBuilder().join(bazQb).query();
		assertEquals(1, results.size());
		assertEquals(bing1.id, results.get(0).id);
		assertEquals(baz1.id, results.get(0).baz.id);
		bazDao.refresh(results.get(0).baz);
		assertEquals(bar1.id, results.get(0).baz.bar.id);
	}

	@Test
	public void testMultipleJoin() throws Exception {
		Dao<Bar, Integer> barDao = createDao(Bar.class, true);
		Dao<Baz, Integer> bazDao = createDao(Baz.class, true);
		Dao<Bing, Integer> bingDao = createDao(Bing.class, true);

		Bar bar1 = new Bar();
		bar1.val = 2234;
		assertEquals(1, barDao.create(bar1));
		Bar bar2 = new Bar();
		bar2.val = 324322234;
		assertEquals(1, barDao.create(bar2));

		Baz baz1 = new Baz();
		baz1.bar = bar1;
		assertEquals(1, bazDao.create(baz1));
		Baz baz2 = new Baz();
		baz2.bar = bar2;
		assertEquals(1, bazDao.create(baz2));

		Bing bing1 = new Bing();
		bing1.baz = baz1;
		assertEquals(1, bingDao.create(bing1));
		Bing bing2 = new Bing();
		bing2.baz = baz1;
		assertEquals(1, bingDao.create(bing2));

		QueryBuilder<Bar, Integer> barQb = barDao.queryBuilder();
		barQb.where().eq(Bar.VAL_FIELD, bar1.val);
		List<Baz> results = bazDao.queryBuilder().query();
		assertEquals(2, results.size());

		QueryBuilder<Bing, Integer> bingQb = bingDao.queryBuilder();
		bingQb.where().eq(Bing.ID_FIELD, bing2.id);
		List<Baz> bingResults = bazDao.queryBuilder().query();
		assertEquals(2, bingResults.size());

		results = bazDao.queryBuilder().join(barQb).join(bingQb).query();
		assertEquals(1, results.size());
		assertEquals(bar1.id, results.get(0).bar.id);
	}

	/* ============================================================== */

	protected void checkPartialIterator(Iterator<PartialData> iterator, List<Integer> ids, List<String> firsts,
			List<String> lasts, boolean firstNull, boolean lastNull) {
		int i = 0;
		while (iterator.hasNext()) {
			PartialData partialData = iterator.next();
			checkPartial(partialData, ids, firsts, lasts, i, firstNull, lastNull);
			i++;
		}
	}

	protected void createPartial(Dao<PartialData, Integer> partialDao, List<Integer> ids, List<String> firsts,
			List<String> lasts, String first, String last) throws SQLException {
		PartialData partial = new PartialData();
		partial.first = first;
		partial.last = last;
		partialDao.create(partial);
		ids.add(partial.id);
		firsts.add(partial.first);
		lasts.add(partial.last);
		checkPartial(partialDao.queryForId(partial.id), ids, firsts, lasts, ids.size() - 1, false, false);
	}

	protected void checkPartialList(List<PartialData> partialList, List<Integer> ids, List<String> firsts,
			List<String> lasts, boolean firstNull, boolean lastNull) {
		assertEquals(partialList.size(), ids.size());
		for (int i = 0; i < partialList.size(); i++) {
			PartialData partial = partialList.get(i);
			assertEquals((int) ids.get(i), partial.id);
			if (firstNull) {
				assertNull(partial.first);
			} else {
				assertEquals(partial.first, firsts.get(i));
			}
			if (lastNull) {
				assertNull(partial.last);
			} else {
				assertEquals(partial.last, lasts.get(i));
			}
		}
	}

	private void checkPartial(PartialData partial, List<Integer> ids, List<String> firsts, List<String> lasts,
			int which, boolean firstNull, boolean lastNull) {
		assertNotNull(partial);
		assertTrue(which >= 0 && which < firsts.size());
		assertEquals((int) ids.get(which), partial.id);
		if (firstNull) {
			assertNull(partial.first);
		} else {
			assertEquals(partial.first, firsts.get(which));
		}
		if (lastNull) {
			assertNull(partial.last);
		} else {
			assertEquals(partial.last, lasts.get(which));
		}
	}

	private Dao<Foo, String> createTestData() throws Exception {
		Dao<Foo, String> fooDao = createDao(Foo.class, true);
		foo1 = new Foo();
		foo1.id = ID_PREFIX + "1";
		foo1.val = LOW_VAL;
		foo1.equal = EQUAL_VAL;
		assertEquals(1, fooDao.create(foo1));
		foo2 = new Foo();
		foo2.id = ID_PREFIX + "2";
		foo2.val = HIGH_VAL;
		foo2.equal = EQUAL_VAL;
		assertEquals(1, fooDao.create(foo2));
		return fooDao;
	}

	protected static class Foo {
		public static final String ID_COLUMN_NAME = "id";
		public static final String VAL_COLUMN_NAME = "val";
		public static final String EQUAL_COLUMN_NAME = "equal";
		public static final String NULL_COLUMN_NAME = "null";

		@DatabaseField(id = true, columnName = ID_COLUMN_NAME)
		public String id;
		@DatabaseField(columnName = VAL_COLUMN_NAME)
		public int val;
		@DatabaseField(columnName = EQUAL_COLUMN_NAME)
		public int equal;
		@DatabaseField(columnName = NULL_COLUMN_NAME)
		public String nullField;

		public Foo() {
			// for ormlite
		}

		@Override
		public String toString() {
			return "Foo:" + id;
		}

		@Override
		public boolean equals(Object other) {
			if (other == null || other.getClass() != getClass())
				return false;
			return id.equals(((Foo) other).id);
		}

		@Override
		public int hashCode() {
			return id.hashCode();
		}
	}

	protected static class PartialData {
		public static final String ID_FIELD_NAME = "id";
		public static final String FIRST_FIELD_NAME = "first";
		public static final String LAST_FIELD_NAME = "last";

		@DatabaseField(generatedId = true, columnName = ID_FIELD_NAME)
		public int id;
		@DatabaseField(columnName = FIRST_FIELD_NAME)
		public String first;
		@DatabaseField(columnName = LAST_FIELD_NAME)
		public String last;
	}

	protected static class Bar {
		public static final String ID_FIELD = "id";
		public static final String VAL_FIELD = "val";
		@DatabaseField(generatedId = true, columnName = ID_FIELD)
		int id;
		@DatabaseField(columnName = VAL_FIELD)
		int val;

		public Bar() {
		}
	}

	protected static class Baz {
		public static final String ID_FIELD = "id";
		public static final String VAL_FIELD = "val";
		public static final String BAR_FIELD = "bar";
		@DatabaseField(generatedId = true, columnName = ID_FIELD)
		int id;
		@DatabaseField(columnName = VAL_FIELD)
		int val;
		@DatabaseField(foreign = true, columnName = BAR_FIELD)
		Bar bar;

		public Baz() {
		}
	}

	protected static class Bing {
		public static final String ID_FIELD = "id";
		public static final String BAZ_FIELD = "baz";
		@DatabaseField(generatedId = true, columnName = ID_FIELD)
		int id;
		@DatabaseField(foreign = true, columnName = BAZ_FIELD)
		Baz baz;

		public Bing() {
		}
	}
}
