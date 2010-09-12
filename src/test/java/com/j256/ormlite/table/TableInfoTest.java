package com.j256.ormlite.table;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.j256.ormlite.BaseOrmLiteTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;

public class TableInfoTest extends BaseOrmLiteTest {

	/**
	 * Test to make sure that we can call a private constructor
	 */
	@Test
	public void testPrivateConstructor() throws Exception {
		Dao<PrivateConstructor, Object> packConstDao = createDao(PrivateConstructor.class, true);
		int id = 12312321;
		PrivateConstructor pack1 = PrivateConstructor.makeOne(id);
		assertEquals(id, pack1.id);
		packConstDao.create(pack1);
		// we should be able to look it up
		PrivateConstructor pack2 = packConstDao.queryForId(id);
		// and the id should match
		assertEquals(id, pack2.id);
	}

	private static class PrivateConstructor {
		@DatabaseField(id = true)
		int id;
		private PrivateConstructor() {
			// make it private
		}
		public static PrivateConstructor makeOne(int id) {
			PrivateConstructor pack = new PrivateConstructor();
			pack.id = id;
			return pack;
		}
	}
}
