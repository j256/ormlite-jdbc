package com.j256.ormlite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DerbyClientServerDatabaseTypeTest extends DerbyEmbeddedDatabaseTypeTest {

	@Test
	public void testGetClientServerDriverClassName() {
		assertEquals("org.apache.derby.jdbc.ClientDriver", new DerbyClientServerDatabaseType().getDriverClassName());
	}

	@Test
	public void testIsDatabaseUrlThisType() {
		assertTrue(new DerbyClientServerDatabaseType()
				.isDatabaseUrlThisType("jdbc:derby://localhost:1527/MyDbTest;create=true';", "derby"));
		assertFalse(new DerbyClientServerDatabaseType().isDatabaseUrlThisType("jdbc:derby:database", "derby"));
	}
}
