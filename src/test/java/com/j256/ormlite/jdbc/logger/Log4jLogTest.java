package com.j256.ormlite.jdbc.logger;

import com.j256.ormlite.jdbc.logger.Log4jLog;
import com.j256.ormlite.logger.BaseLogTest;

public class Log4jLogTest extends BaseLogTest {

	public Log4jLogTest() {
		super(new Log4jLog("Log4jLogTest"));
	}
}
