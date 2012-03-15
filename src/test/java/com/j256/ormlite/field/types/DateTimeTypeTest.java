package com.j256.ormlite.field.types;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import org.joda.time.DateTime;
import org.junit.Test;

import com.j256.ormlite.BaseCoreTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.support.DatabaseResults;

public class DateTimeTypeTest extends BaseCoreTest {

	@Test
	public void testJavaToSqlArg() throws Exception {
		DateTime dateTime = new DateTime();
		assertEquals(dateTime.getMillis(), DateTimeType.getSingleton().javaToSqlArg(null, dateTime));
	}

	@Test
	public void testParseDefaultString() {
		Long value = 423424234234L;
		assertEquals(value, DateTimeType.getSingleton().parseDefaultString(null, value.toString()));
	}

	@Test
	public void testResultToSqlArg() throws Exception {
		DatabaseResults results = createMock(DatabaseResults.class);
		int col = 21;
		long value = 2094234324L;
		expect(results.getLong(col)).andReturn(value);
		replay(results);
		assertEquals(new DateTime(value), DateTimeType.getSingleton().resultToJava(null, results, col));
	}

	@Test
	public void testPersist() throws Exception {
		Dao<StoreDateTime, Object> dao = createDao(StoreDateTime.class, true);
		StoreDateTime foo = new StoreDateTime();
		foo.dateTime = new DateTime();
		assertEquals(1, dao.create(foo));

		StoreDateTime result = dao.queryForId(foo.id);
		assertEquals(result.dateTime, foo.dateTime);
	}

	private static class StoreDateTime {
		@DatabaseField(generatedId = true)
		int id;

		@DatabaseField
		DateTime dateTime;

		public StoreDateTime() {
		}
	}
}
