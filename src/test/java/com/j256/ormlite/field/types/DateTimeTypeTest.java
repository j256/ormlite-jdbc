package com.j256.ormlite.field.types;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import org.joda.time.DateTime;
import org.junit.Test;

import com.j256.ormlite.support.DatabaseResults;

public class DateTimeTypeTest {

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
}
