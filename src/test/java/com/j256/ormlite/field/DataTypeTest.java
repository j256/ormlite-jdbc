package com.j256.ormlite.field;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import com.j256.ormlite.BaseOrmLiteJdbcTest;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.stmt.StatementBuilder.StatementType;
import com.j256.ormlite.support.CompiledStatement;
import com.j256.ormlite.support.DatabaseResults;
import com.j256.ormlite.table.DatabaseTable;

public class DataTypeTest extends BaseOrmLiteJdbcTest {

	private static final String TABLE_NAME = "foo";

	private static final String STRING_COLUMN = "id";
	private static final String BOOLEAN_COLUMN = "bool";
	private static final String DATE_COLUMN = "date";
	private static final String BYTE_COLUMN = "byteField";
	private static final String SHORT_COLUMN = "shortField";
	private static final String INT_COLUMN = "intField";
	private static final String LONG_COLUMN = "longField";
	private static final String FLOAT_COLUMN = "floatField";
	private static final String DOUBLE_COLUMN = "doubleField";
	private static final String ENUM_COLUMN = "enum";

	private static final String DATE_FORMAT = "MM/dd/yyyy";
	private static final String DATE_STRING = "10/10/2010";
	private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat(DATE_FORMAT);
	private static Date DATE;
	private static Timestamp TIMESTAMP;
	private static final int COLUMN = 0;
	private static final String BAD_DATE_STRING = "badDateString";
	private static final String INT_STRING = "5";
	private static final String FLOAT_STRING = "5.0";

	static {
		try {
			DATE = DATE_FORMATTER.parse(DATE_STRING);
			TIMESTAMP = new Timestamp(DATE.getTime());
		} catch (ParseException pe) {
			new RuntimeException("Unable to parse date from " + DATE_STRING);
		}
	}

	@Test
	public void testString() throws Exception {
		Dao<LocalString, Object> fooDao = createDao(LocalString.class, true);
		String string = "str";
		LocalString foo = new LocalString();
		foo.string = string;
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		assertEquals(string, DataType.STRING.resultToJava(null, results, results.findColumn(STRING_COLUMN)));
		String def = "opqejq";
		assertSame(def, DataType.STRING.parseDefaultString(null, def));
		assertFalse(DataType.STRING.isValidGeneratedType());
		assertFalse(DataType.STRING.isStreamType());
		assertFalse(DataType.STRING.isNumber());
		assertTrue(DataType.STRING.isEscapeDefaultValue());
	}

	@Test
	public void testBoolean1() throws Exception {
		Dao<LocalBoolean, Object> fooDao = createDao(LocalBoolean.class, true);
		boolean bool = true;
		LocalBoolean foo = new LocalBoolean();
		foo.bool = bool;
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		FieldType fieldType =
				FieldType.createFieldType(databaseType, "table", LocalBoolean.class.getDeclaredField(BOOLEAN_COLUMN));
		assertEquals(bool, DataType.BOOLEAN.resultToJava(fieldType, results, results.findColumn(BOOLEAN_COLUMN)));
		assertFalse(DataType.BOOLEAN.isValidGeneratedType());
		assertFalse(DataType.BOOLEAN.isStreamType());
		assertFalse(DataType.BOOLEAN.isNumber());
		assertFalse(DataType.BOOLEAN.isEscapeDefaultValue());
	}

	@Test
	public void testDate() throws Exception {
		Dao<LocalDate, Object> fooDao = createDao(LocalDate.class, true);
		Date date = new Date();
		LocalDate foo = new LocalDate();
		foo.date = date;
		assertEquals(1, fooDao.create(foo));
		Field[] fields = LocalDate.class.getDeclaredFields();
		assertTrue(fields.length > 0);
		Field dateField = fields[0];

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		assertEquals(date, DataType.JAVA_DATE.resultToJava(null, results, results.findColumn(DATE_COLUMN)));
		assertEquals(new Timestamp(date.getTime()), DataType.JAVA_DATE.javaToArg(null, date));
		assertFalse(DataType.JAVA_DATE.isValidGeneratedType());
		String format = "yyyy-MM-dd HH:mm:ss.SSSSSS";
		DateFormat dateFormat = new SimpleDateFormat(format);
		FieldType fieldType = FieldType.createFieldType(databaseType, TABLE_NAME, dateField);
		assertEquals(new Timestamp(date.getTime()), DataType.JAVA_DATE.parseDefaultString(fieldType,
				dateFormat.format(date)));
		assertFalse(DataType.JAVA_DATE.isValidGeneratedType());
		assertFalse(DataType.JAVA_DATE.isStreamType());
		assertFalse(DataType.JAVA_DATE.isNumber());
		assertTrue(DataType.JAVA_DATE.isEscapeDefaultValue());
	}

	@Test(expected = SQLException.class)
	public void testDateBadFormat() throws Exception {
		FieldType fieldType =
				FieldType.createFieldType(databaseType, "table", DateBadFormat.class.getDeclaredField(DATE_COLUMN));
		DataType.JAVA_DATE.parseDefaultString(fieldType, "xxx");
	}

	@Test
	public void testByte1() throws Exception {
		Dao<LocalByte, Object> fooDao = createDao(LocalByte.class, true);
		byte byteField = 123;
		LocalByte foo = new LocalByte();
		foo.byteField = byteField;
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		FieldType fieldType =
				FieldType.createFieldType(databaseType, "table", LocalByte.class.getDeclaredField(BYTE_COLUMN));
		assertEquals(byteField, DataType.BYTE.resultToJava(fieldType, results, results.findColumn(BYTE_COLUMN)));
		assertFalse(DataType.BYTE.isValidGeneratedType());
		assertFalse(DataType.BYTE.isStreamType());
		assertTrue(DataType.BYTE.isNumber());
		assertFalse(DataType.BYTE.isEscapeDefaultValue());
	}

	@Test
	public void testShort1() throws Exception {
		Dao<LocalShort, Object> fooDao = createDao(LocalShort.class, true);
		short shortField = 12312;
		LocalShort foo = new LocalShort();
		foo.shortField = shortField;
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		FieldType fieldType =
				FieldType.createFieldType(databaseType, "table", LocalShort.class.getDeclaredField(SHORT_COLUMN));
		assertEquals(shortField, DataType.SHORT.resultToJava(fieldType, results, results.findColumn(SHORT_COLUMN)));
		assertFalse(DataType.SHORT.isValidGeneratedType());
		assertFalse(DataType.SHORT.isStreamType());
		assertTrue(DataType.SHORT.isNumber());
		assertFalse(DataType.SHORT.isEscapeDefaultValue());
	}

	@Test
	public void testInt() throws Exception {
		Dao<LocalInt, Object> fooDao = createDao(LocalInt.class, true);
		int integer = 313213123;
		LocalInt foo = new LocalInt();
		foo.intField = integer;
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		FieldType fieldType =
				FieldType.createFieldType(databaseType, "table", LocalInt.class.getDeclaredField(INT_COLUMN));
		assertEquals(integer, DataType.INTEGER.resultToJava(fieldType, results, results.findColumn(INT_COLUMN)));
		assertTrue(DataType.INTEGER.isValidGeneratedType());
		assertFalse(DataType.INTEGER.isStreamType());
		assertTrue(DataType.INTEGER.isNumber());
		assertFalse(DataType.INTEGER.isEscapeDefaultValue());
	}

	@Test
	public void testLong1() throws Exception {
		Dao<LocalLong, Object> fooDao = createDao(LocalLong.class, true);
		long longInt = 13312321312312L;
		LocalLong foo = new LocalLong();
		foo.longField = longInt;
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		FieldType fieldType =
				FieldType.createFieldType(databaseType, "table", LocalLong.class.getDeclaredField(LONG_COLUMN));
		assertEquals(longInt, DataType.LONG.resultToJava(fieldType, results, results.findColumn(LONG_COLUMN)));
		assertTrue(DataType.LONG.isValidGeneratedType());
		assertFalse(DataType.LONG.isStreamType());
		assertTrue(DataType.LONG.isNumber());
		assertFalse(DataType.LONG.isEscapeDefaultValue());
	}

	@Test
	public void testFloat1() throws Exception {
		Dao<LocalFloat, Object> fooDao = createDao(LocalFloat.class, true);
		float floatField = 1331.221F;
		LocalFloat foo = new LocalFloat();
		foo.floatField = floatField;
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		FieldType fieldType =
				FieldType.createFieldType(databaseType, "table", LocalFloat.class.getDeclaredField(FLOAT_COLUMN));
		assertEquals(floatField, DataType.FLOAT.resultToJava(fieldType, results, results.findColumn(FLOAT_COLUMN)));
		assertFalse(DataType.FLOAT.isValidGeneratedType());
		assertFalse(DataType.FLOAT.isStreamType());
		assertTrue(DataType.FLOAT.isNumber());
		assertFalse(DataType.FLOAT.isEscapeDefaultValue());
	}

	@Test
	public void testDouble1() throws Exception {
		Dao<LocalDouble, Object> fooDao = createDao(LocalDouble.class, true);
		double doubleField = 13313323131.221;
		LocalDouble foo = new LocalDouble();
		foo.doubleField = doubleField;
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		FieldType fieldType =
				FieldType.createFieldType(databaseType, "table", LocalDouble.class.getDeclaredField(DOUBLE_COLUMN));
		assertEquals(doubleField, DataType.DOUBLE.resultToJava(fieldType, results, results.findColumn(DOUBLE_COLUMN)));
		assertFalse(DataType.DOUBLE.isValidGeneratedType());
		assertFalse(DataType.DOUBLE.isStreamType());
		assertTrue(DataType.DOUBLE.isNumber());
		assertFalse(DataType.DOUBLE.isEscapeDefaultValue());
	}

	@Test
	public void testEnum() throws Exception {
		Dao<LocalEnum, Object> fooDao = createDao(LocalEnum.class, true);
		OurEnum ourEnum = OurEnum.SECOND;
		LocalEnum foo = new LocalEnum();
		foo.ourEnum = ourEnum;
		assertEquals(1, fooDao.create(foo));

		Field[] fields = LocalEnum.class.getDeclaredFields();
		assertTrue(fields.length > 0);
		FieldType fieldType = FieldType.createFieldType(databaseType, TABLE_NAME, fields[0]);

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		assertEquals(ourEnum, DataType.ENUM_STRING.resultToJava(fieldType, results, results.findColumn(ENUM_COLUMN)));
		assertFalse(DataType.ENUM_STRING.isValidGeneratedType());
		assertFalse(DataType.ENUM_STRING.isStreamType());
		assertFalse(DataType.ENUM_STRING.isNumber());
		assertTrue(DataType.ENUM_STRING.isEscapeDefaultValue());
	}

	@Test
	public void testEnumInt() throws Exception {
		Dao<LocalEnumInt, Object> fooDao = createDao(LocalEnumInt.class, true);
		OurEnum ourEnum = OurEnum.SECOND;
		LocalEnumInt foo = new LocalEnumInt();
		foo.ourEnum = ourEnum;
		assertEquals(1, fooDao.create(foo));

		Field[] fields = LocalEnum.class.getDeclaredFields();
		assertTrue(fields.length > 0);
		FieldType fieldType = FieldType.createFieldType(databaseType, TABLE_NAME, fields[0]);

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());
		assertEquals(ourEnum, DataType.ENUM_INTEGER.resultToJava(fieldType, results, results.findColumn(ENUM_COLUMN)));
		assertFalse(DataType.ENUM_INTEGER.isValidGeneratedType());
		assertFalse(DataType.ENUM_INTEGER.isStreamType());
		assertTrue(DataType.ENUM_INTEGER.isNumber());
		assertFalse(DataType.ENUM_INTEGER.isEscapeDefaultValue());
	}

	@Test
	public void testUnknownGetResult() throws Exception {
		Dao<LocalLong, Object> fooDao = createDao(LocalLong.class, true);
		LocalLong foo = new LocalLong();
		assertEquals(1, fooDao.create(foo));

		CompiledStatement stmt =
				connectionSource.getReadOnlyConnection().compileStatement("select * from " + TABLE_NAME,
						StatementType.SELECT);
		DatabaseResults results = stmt.executeQuery();
		assertTrue(results.next());

		assertNull(DataType.UNKNOWN.resultToJava(null, results, 1));
		assertFalse(DataType.UNKNOWN.isValidGeneratedType());
		assertFalse(DataType.UNKNOWN.isStreamType());
		assertFalse(DataType.UNKNOWN.isNumber());
		assertTrue(DataType.UNKNOWN.isEscapeDefaultValue());
	}

	@Test
	public void testUnknownClass() {
		assertEquals(DataType.UNKNOWN, DataType.lookupClass(getClass()));
	}

	@Test
	public void testSerializableLookup() {
		assertEquals(DataType.SERIALIZABLE, DataType.lookupClass(Serializable.class));
	}

	@Test
	public void testEnumLookup() {
		assertEquals(DataType.ENUM_STRING, DataType.lookupClass(Enum.class));
	}

	@Test
	public void testBoolean() throws Exception {
		DataType type = DataType.BOOLEAN;
		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		Boolean booleanResult = Boolean.TRUE;
		expect(results.getBoolean(COLUMN)).andReturn(booleanResult);
		replay(results);
		assertEquals(booleanResult, type.resultToJava(null, results, COLUMN));
		verify(results);

		assertFalse(type.isEscapeDefaultValue());
		assertTrue(type.isPrimitive());
		assertEquals(Boolean.TRUE, type.parseDefaultString(null, "true"));
		assertEquals(Boolean.FALSE, type.parseDefaultString(null, "false"));
		assertNull(type.resultToId(null, 0));
	}

	@Test
	public void testBooleanObj() throws Exception {
		DataType type = DataType.BOOLEAN_OBJ;
		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		Boolean booleanResult = Boolean.TRUE;
		expect(results.getBoolean(COLUMN)).andReturn(booleanResult);
		replay(results);
		assertEquals(booleanResult, type.resultToJava(null, results, COLUMN));
		verify(results);

		assertFalse(type.isEscapeDefaultValue());
		assertEquals(Boolean.TRUE, type.parseDefaultString(null, "true"));
		assertEquals(Boolean.FALSE, type.parseDefaultString(null, "false"));
	}

	@Test
	public void testJavaDate() throws Exception {
		DataType type = DataType.JAVA_DATE;
		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getTimestamp(COLUMN)).andReturn(TIMESTAMP);
		replay(results);
		assertEquals(DATE, type.resultToJava(null, results, COLUMN));
		verify(results);

		Timestamp timestamp = new Timestamp(DATE_FORMATTER.parse(DATE_STRING).getTime());
		assertEquals(timestamp, type.parseDefaultString(getFieldType("date"), DATE_STRING));

		timestamp = (Timestamp) type.javaToArg(null, DATE);
		assertEquals(TIMESTAMP, timestamp);
	}

	@Test
	public void testJavaDateLong() throws Exception {
		DataType type = DataType.JAVA_DATE_LONG;
		long millis = 5;
		Date date = new Date(millis);

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getLong(COLUMN)).andReturn(new Long(millis));
		replay(results);
		assertEquals(date, type.resultToJava(null, results, COLUMN));
		verify(results);

		Long expectedLong = (Long) type.javaToArg(null, date);
		assertEquals(millis, expectedLong.longValue());
		assertTrue(type.isNumber());
		String longString = "255";
		assertEquals(new Long(longString), type.parseDefaultString(null, longString));
	}

	@Test(expected = SQLException.class)
	public void testBadJavaDateLong() throws Exception {
		DataType.JAVA_DATE_LONG.parseDefaultString(null, "notALong");
	}

	@Test
	public void testJavaDateString() throws Exception {
		DataType type = DataType.JAVA_DATE_STRING;
		FieldType fieldType = getFieldType("date");
		assertEquals(DATE_STRING, type.javaToArg(fieldType, DATE));
		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getString(COLUMN)).andReturn(DATE_STRING);
		replay(results);
		assertEquals(DATE, type.resultToJava(fieldType, results, COLUMN));
		verify(results);
	}

	@Test(expected = SQLException.class)
	public void testJavaBadDateString() throws Exception {
		DataType type = DataType.JAVA_DATE_STRING;
		FieldType fieldType = getFieldType("date");
		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getString(COLUMN)).andReturn(BAD_DATE_STRING);
		replay(results);
		type.resultToJava(fieldType, results, COLUMN);
		verify(results);
	}

	@Test
	public void testJavaDateStringParseDefaultString() throws Exception {
		DataType type = DataType.JAVA_DATE_STRING;
		FieldType fieldType = getFieldType("date");
		assertEquals(DATE_STRING, type.parseDefaultString(fieldType, DATE_STRING));
	}

	@Test(expected = SQLException.class)
	public void testJavaDateStringParseBadDefaultString() throws Exception {
		DataType type = DataType.JAVA_DATE_STRING;
		FieldType fieldType = getFieldType("date");
		type.parseDefaultString(fieldType, BAD_DATE_STRING);
	}

	@Test
	public void testByte() throws Exception {
		DataType type = DataType.BYTE;
		byte testByte = Byte.parseByte(INT_STRING);
		assertEquals(new Byte(INT_STRING), type.parseDefaultString(null, INT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getByte(COLUMN)).andReturn(testByte);
		replay(results);
		assertEquals(testByte, type.resultToJava(getFieldType("count"), results, COLUMN));
		verify(results);

		assertTrue(type.isNumber());
		assertTrue(type.isPrimitive());
	}

	@Test
	public void testByteObj() throws Exception {
		DataType type = DataType.BYTE_OBJ;
		byte testByte = Byte.parseByte(INT_STRING);
		assertEquals(new Byte(INT_STRING), type.parseDefaultString(null, INT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getByte(COLUMN)).andReturn(testByte);
		replay(results);
		assertEquals(testByte, type.resultToJava(getFieldType("count"), results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertFalse(type.isPrimitive());
	}

	@Test
	public void testShort() throws Exception {
		DataType type = DataType.SHORT;
		short testShort = Short.parseShort(INT_STRING);
		assertEquals(new Short(INT_STRING), type.parseDefaultString(null, INT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getShort(COLUMN)).andReturn(testShort);
		replay(results);
		assertEquals(testShort, type.resultToJava(getFieldType("count"), results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertTrue(type.isPrimitive());
	}

	@Test
	public void testShortObj() throws Exception {
		DataType type = DataType.SHORT_OBJ;
		short testShort = Short.parseShort(INT_STRING);
		assertEquals(new Short(INT_STRING), type.parseDefaultString(null, INT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getShort(COLUMN)).andReturn(testShort);
		replay(results);
		assertEquals(testShort, type.resultToJava(getFieldType("count"), results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertFalse(type.isPrimitive());
	}

	@Test
	public void testInteger() throws Exception {
		DataType type = DataType.INTEGER;
		int testInt = Integer.parseInt(INT_STRING);
		assertEquals(new Integer(INT_STRING), type.parseDefaultString(null, INT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getInt(COLUMN)).andReturn(testInt);
		expect(results.getInt(COLUMN)).andReturn(testInt);
		replay(results);
		assertEquals(testInt, type.resultToJava(getFieldType("count"), results, COLUMN));
		assertEquals(testInt, type.resultToId(results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertTrue(type.isPrimitive());
	}

	@Test
	public void testIntegerObj() throws Exception {
		DataType type = DataType.INTEGER_OBJ;
		int testInt = Integer.parseInt(INT_STRING);
		assertEquals(new Integer(INT_STRING), type.parseDefaultString(null, INT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getInt(COLUMN)).andReturn(testInt);
		expect(results.getInt(COLUMN)).andReturn(testInt);
		replay(results);
		assertEquals(testInt, type.resultToJava(getFieldType("count"), results, COLUMN));
		assertEquals(testInt, type.resultToId(results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertFalse(type.isPrimitive());
	}

	@Test
	public void testLong() throws Exception {
		DataType type = DataType.LONG;
		long testLong = Long.parseLong(INT_STRING);
		assertEquals(new Long(INT_STRING), type.parseDefaultString(null, INT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getLong(COLUMN)).andReturn(testLong);
		expect(results.getLong(COLUMN)).andReturn(testLong);
		replay(results);
		assertEquals(testLong, type.resultToJava(getFieldType("count"), results, COLUMN));
		assertEquals(testLong, type.resultToId(results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertTrue(type.isPrimitive());
	}

	@Test
	public void testLongObj() throws Exception {
		DataType type = DataType.LONG_OBJ;
		long testLong = Long.parseLong(INT_STRING);
		assertEquals(new Long(INT_STRING), type.parseDefaultString(null, INT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getLong(COLUMN)).andReturn(testLong);
		expect(results.getLong(COLUMN)).andReturn(testLong);
		replay(results);
		assertEquals(testLong, type.resultToJava(getFieldType("count"), results, COLUMN));
		assertEquals(testLong, type.resultToId(results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertFalse(type.isPrimitive());
	}

	@Test
	public void testFloat() throws Exception {
		DataType type = DataType.FLOAT;
		float testFloat = Float.parseFloat(FLOAT_STRING);
		assertEquals(new Float(FLOAT_STRING), type.parseDefaultString(null, FLOAT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getFloat(COLUMN)).andReturn(testFloat);
		replay(results);
		assertEquals(testFloat, type.resultToJava(getFieldType("count"), results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertTrue(type.isPrimitive());
	}

	@Test
	public void testFloatObj() throws Exception {
		DataType type = DataType.FLOAT_OBJ;
		float testFloat = Float.parseFloat(FLOAT_STRING);
		assertEquals(new Float(FLOAT_STRING), type.parseDefaultString(null, FLOAT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getFloat(COLUMN)).andReturn(testFloat);
		replay(results);
		assertEquals(testFloat, type.resultToJava(getFieldType("count"), results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertFalse(type.isPrimitive());
	}

	@Test
	public void testDouble() throws Exception {
		DataType type = DataType.DOUBLE;
		double testDouble = Double.parseDouble(FLOAT_STRING);
		assertEquals(new Double(FLOAT_STRING), type.parseDefaultString(null, FLOAT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getDouble(COLUMN)).andReturn(testDouble);
		replay(results);
		assertEquals(testDouble, type.resultToJava(getFieldType("count"), results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertTrue(type.isPrimitive());
	}

	@Test
	public void testDoubleObj() throws Exception {
		DataType type = DataType.DOUBLE_OBJ;
		double testDouble = Double.parseDouble(FLOAT_STRING);
		assertEquals(new Double(FLOAT_STRING), type.parseDefaultString(null, FLOAT_STRING));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getDouble(COLUMN)).andReturn(testDouble);
		replay(results);
		assertEquals(testDouble, type.resultToJava(getFieldType("count"), results, COLUMN));
		verify(results);
		assertTrue(type.isNumber());
		assertFalse(type.isPrimitive());
	}

	@Test(expected = SQLException.class)
	public void testSerializableDefaultNull() throws Exception {
		DataType type = DataType.SERIALIZABLE;
		assertTrue(type.isStreamType());
		type.parseDefaultString(null, null);
	}

	@Test
	public void testSerializable() throws Exception {
		DataType type = DataType.SERIALIZABLE;
		Integer serializable = new Integer(0);
		byte[] bytes = (byte[]) type.javaToArg(null, serializable);
		ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(bytes));
		Object val = stream.readObject();
		assertEquals(serializable, val);

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getBytes(COLUMN)).andReturn(bytes);
		expect(results.getBytes(COLUMN)).andReturn(null);
		replay(results);
		assertEquals(serializable, type.resultToJava(getFieldType("serializable"), results, COLUMN));
		assertNull(type.resultToJava(getFieldType("serializable"), results, COLUMN));
		verify(results);
		assertFalse(type.isNumber());
		assertFalse(type.isPrimitive());
	}

	@Test(expected = SQLException.class)
	public void testBadSerializableBytes() throws Exception {
		DataType type = DataType.SERIALIZABLE;
		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getBytes(COLUMN)).andReturn(new byte[] { 1, 2, 3 });
		replay(results);
		type.resultToJava(getFieldType("serializable"), results, COLUMN);
	}

	@Test(expected = SQLException.class)
	public void testSerializableBadObject() throws Exception {
		DataType type = DataType.SERIALIZABLE;
		type.javaToArg(null, new LocalString());
	}

	@Test
	public void testSerializableIsValid() throws Exception {
		DataType type = DataType.SERIALIZABLE;
		assertFalse(type.isValidForType(LocalString.class));
		assertTrue(type.isValidForType(Serializable.class));
	}

	@Test
	public void testEnumString() throws Exception {
		DataType type = DataType.ENUM_STRING;
		AnotherEnum anotherEnum = AnotherEnum.A;
		assertEquals(anotherEnum.name(), type.javaToArg(null, anotherEnum));
		String defaultString = "default";
		assertEquals(defaultString, type.parseDefaultString(null, defaultString));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getString(COLUMN)).andReturn(AnotherEnum.A.name());
		replay(results);
		assertEquals(AnotherEnum.A, type.resultToJava(getFieldType("grade"), results, COLUMN));
		verify(results);
		assertFalse(type.isNumber());
	}

	@Test
	public void testEnumInteger() throws Exception {
		DataType type = DataType.ENUM_INTEGER;
		AnotherEnum anotherEnum = AnotherEnum.A;
		assertEquals(anotherEnum.ordinal(), type.javaToArg(null, anotherEnum));
		String integerString = "5";
		Integer defaultInteger = new Integer(integerString);
		assertEquals(defaultInteger, type.parseDefaultString(null, integerString));

		DatabaseResults results = (DatabaseResults) createMock(DatabaseResults.class);
		expect(results.getInt(COLUMN)).andReturn(AnotherEnum.A.ordinal());
		replay(results);
		assertEquals(AnotherEnum.A, type.resultToJava(getFieldType("grade"), results, COLUMN));
		verify(results);

		assertTrue(type.isNumber());
	}

	@Test
	public void testUnknown() throws Exception {
		DataType type = DataType.UNKNOWN;
		String defaultString = "5";
		assertNull(type.javaToArg(null, defaultString));
		assertNull(type.parseDefaultString(null, defaultString));
		assertNull(type.resultToJava(null, null, 0));
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalString {
		@DatabaseField(id = true, columnName = STRING_COLUMN)
		String string;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalBoolean {
		@DatabaseField(columnName = BOOLEAN_COLUMN)
		boolean bool;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalDate {
		@DatabaseField(columnName = DATE_COLUMN)
		Date date;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class DateBadFormat {
		@DatabaseField(columnName = DATE_COLUMN, format = "yyyy")
		Date date;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalByte {
		@DatabaseField(columnName = BYTE_COLUMN)
		byte byteField;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalShort {
		@DatabaseField(columnName = SHORT_COLUMN)
		short shortField;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalInt {
		@DatabaseField(columnName = INT_COLUMN)
		int intField;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalLong {
		@DatabaseField(columnName = LONG_COLUMN)
		long longField;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalFloat {
		@DatabaseField(columnName = FLOAT_COLUMN)
		float floatField;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalDouble {
		@DatabaseField(columnName = DOUBLE_COLUMN)
		double doubleField;;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalEnum {
		@DatabaseField(columnName = ENUM_COLUMN)
		OurEnum ourEnum;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalEnumInt {
		@DatabaseField(columnName = ENUM_COLUMN, dataType = DataType.ENUM_INTEGER)
		OurEnum ourEnum;
	}

	@DatabaseTable(tableName = TABLE_NAME)
	protected static class LocalEnumInt2 {
		@DatabaseField(columnName = ENUM_COLUMN, dataType = DataType.ENUM_INTEGER)
		OurEnum2 ourEnum;
	}

	private enum OurEnum {
		FIRST,
		SECOND, ;
	}

	private enum OurEnum2 {
		FIRST, ;
	}

	protected enum AnotherEnum {
		A,
		B,
		// end
		;
	}

	private FieldType getFieldType(String fieldName) throws Exception {
		return FieldType.createFieldType(databaseType, "Foo", Foo.class.getDeclaredField(fieldName));
	}

	protected static class Foo {
		@DatabaseField
		String name;
		@DatabaseField
		int count;
		@DatabaseField
		AnotherEnum grade;
		@DatabaseField(format = DATE_FORMAT)
		Date date;
		@DatabaseField
		Integer serializable;
	}
}
