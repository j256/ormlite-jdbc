package com.j256.ormlite.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.j256.ormlite.BaseJdbcTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.GenericRawResults;
import com.j256.ormlite.dao.RawRowMapper;
import com.j256.ormlite.field.DatabaseField;

public class JdbcRawResultsImplTest extends BaseJdbcTest {

	@Test
	public void testCustomColumnNames() throws Exception {
		Dao<Foo, Integer> dao = createDao(Foo.class, true);
		Foo foo = new Foo();
		foo.val = 1213213;
		assertEquals(1, dao.create(foo));

		final String idName = "SOME_ID";
		final String valName = "SOME_VAL";
		final AtomicBoolean gotResult = new AtomicBoolean(false);
		GenericRawResults<Object> results = dao.queryRaw("select id as " + idName + ", val as " + valName + " from foo",
				new RawRowMapper<Object>() {
					@Override
					public Object mapRow(String[] columnNames, String[] resultColumns) {
						assertEquals(idName, columnNames[0]);
						assertEquals(valName, columnNames[1]);
						gotResult.set(true);
						return new Object();
					}
				});
		List<Object> list = results.getResults();
		assertNotNull(list);
		assertEquals(1, list.size());
		assertTrue(gotResult.get());
	}

	protected static class Foo {
		@DatabaseField(generatedId = true)
		public int id;
		@DatabaseField
		public int val;
	}
}
