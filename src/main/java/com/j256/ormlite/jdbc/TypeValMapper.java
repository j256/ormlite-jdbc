package com.j256.ormlite.jdbc;

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import com.j256.ormlite.field.SqlType;

/**
 * Map from {@link SqlType} to the constants in the {@link Types} class.
 * 
 * @author graywatson
 */
public class TypeValMapper {

	private static final Map<SqlType, int[]> typeToValMap = new HashMap<SqlType, int[]>();

	static {
		for (SqlType sqlType : SqlType.values()) {
			int[] values;
			switch (sqlType) {
				case STRING:
					values = new int[] { Types.VARCHAR };
					break;
				case LONG_STRING:
					values = new int[] { Types.LONGVARCHAR };
					break;
				case DATE:
					values = new int[] { Types.TIMESTAMP };
					break;
				case BOOLEAN:
					values = new int[] { Types.BOOLEAN };
					break;
				case CHAR:
					values = new int[] { Types.CHAR };
					break;
				case BYTE:
					values = new int[] { Types.TINYINT };
					break;
				case BYTE_ARRAY:
					values = new int[] { Types.VARBINARY };
					break;
				case SHORT:
					values = new int[] { Types.SMALLINT };
					break;
				case INTEGER:
					values = new int[] { Types.INTEGER };
					break;
				case LONG:
					values = new int[] { Types.BIGINT };
					break;
				case FLOAT:
					values = new int[] { Types.FLOAT };
					break;
				case DOUBLE:
					values = new int[] { Types.DOUBLE };
					break;
				case SERIALIZABLE:
					values = new int[] { Types.VARBINARY };
					break;
				case BLOB:
					// the following do not need to be handled except in specific situations
					values = new int[] { Types.BLOB };
					break;
				case BIG_DECIMAL:
					values = new int[] { Types.DECIMAL, Types.NUMERIC };
					break;
				case UUID:
					values = new int[] { Types.OTHER };
					break;
				case OTHER:
					values = new int[] { Types.OTHER };
					break;
				case UNKNOWN:
					values = new int[] {};
					break;
				default:
					throw new IllegalArgumentException("No JDBC mapping for unknown SqlType " + sqlType);
			}
			typeToValMap.put(sqlType, values);
		}
	}

	/**
	 * Returns the primary type value associated with the SqlType argument.
	 */
	public static int getTypeValForSqlType(SqlType sqlType) throws SQLException {
		int[] typeVals = typeToValMap.get(sqlType);
		if (typeVals == null) {
			throw new SQLException("SqlType is unknown to type val mapping: " + sqlType);
		}
		if (typeVals.length == 0) {
			throw new SQLException("SqlType does not have any JDBC type value mapping: " + sqlType);
		} else {
			return typeVals[0];
		}
	}

	/**
	 * Returns the SqlType value associated with the typeVal argument. Can be slow-er.
	 */
	public static SqlType getSqlTypeForTypeVal(int typeVal) {
		// iterate through to save on the extra HashMap since only for errors
		for (Map.Entry<SqlType, int[]> entry : typeToValMap.entrySet()) {
			for (int val : entry.getValue()) {
				if (val == typeVal) {
					return entry.getKey();
				}
			}
		}
		return SqlType.UNKNOWN;
	}
}
