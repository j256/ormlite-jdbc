package com.j256.ormlite.jdbc.db;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

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
