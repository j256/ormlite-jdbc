package com.j256.ormlite.examples.datapersister;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.field.types.DateType;
import com.j256.ormlite.support.DatabaseResults;

/**
 * A custom persister that tweaks how the Date is stored in the database. If the date is before 1970 then this just
 * return null instead of throwing some sort of out-of-range error. This is useful because sometimes we get low values
 * of SQL timestamps that are stored in existing schemas which can't be mapped to a Date.
 * 
 * <p>
 * You would <i>not</i> want this to override the default behavior for Date so you would use this by specifying the
 * class in {@link DatabaseField#persisterClass()}.
 * </p>
 * 
 * @author graywatson
 */
public class MyDatePersister extends DateType {

	private static final MyDatePersister singleTon = new MyDatePersister();
	@SuppressWarnings("deprecation")
	private static final Timestamp ZERO_TIMESTAMP = new Timestamp(1970, 0, 0, 0, 0, 0, 0);

	private MyDatePersister() {
		super(SqlType.DATE, new Class<?>[] { Date.class });
	}

	public static MyDatePersister getSingleton() {
		return singleTon;
	}

	@Override
	public Object resultToSqlArg(FieldType fieldType, DatabaseResults results, int columnPos) throws SQLException {
		Timestamp timestamp = results.getTimestamp(columnPos);
		if (timestamp == null || ZERO_TIMESTAMP.after(timestamp)) {
			return null;
		} else {
			return timestamp.getTime();
		}
	}

	@Override
	public Object sqlArgToJava(FieldType fieldType, Object sqlArg, int columnPos) {
		if (sqlArg == null) {
			return null;
		} else {
			return new Date((Long) sqlArg);
		}
	}
}
