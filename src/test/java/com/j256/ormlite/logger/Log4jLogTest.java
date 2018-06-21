package com.j256.ormlite.logger;

import com.j256.ormlite.core.logger.BaseLogTest;

public class Log4jLogTest extends BaseLogTest {

	public Log4jLogTest() {
		super(new Log4jLog("Log4jLogTest"));
	}
}
