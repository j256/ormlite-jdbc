package com.j256.ormlite.jdbc.db;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.j256.ormlite.jdbc.db.DerbyClientServerDatabaseType;

public class DerbyClientServerDatabaseTypeTest extends DerbyEmbeddedDatabaseTypeTest {

	@Test
	public void testGetClientServerDriverClassName() {
		assertArrayEquals(new String[] { "org.apache.derby.jdbc.ClientDriver" },
				new DerbyClientServerDatabaseType().getDriverClassNames());
	}

	@Test
	public void testIsDatabaseUrlThisType() {
		assertTrue(new DerbyClientServerDatabaseType()
				.isDatabaseUrlThisType("jdbc:derby://localhost:1527/MyDbTest;create=true';", "derby"));
		assertFalse(new DerbyClientServerDatabaseType().isDatabaseUrlThisType("jdbc:derby:database", "derby"));
	}
}
