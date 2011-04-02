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

	private static final Map<SqlType, Integer[]> typeToValMap = new HashMap<SqlType, Integer[]>();

	static {
		for (SqlType sqlType : SqlType.values()) {
			switch (sqlType) {
				case STRING :
					typeToValMap.put(sqlType, new Integer[] { Types.VARCHAR });
					break;
				case LONG_STRING :
					typeToValMap.put(sqlType, new Integer[] { Types.LONGVARCHAR });
					break;
				case DATE :
					typeToValMap.put(sqlType, new Integer[] { Types.TIMESTAMP });
					break;
				case BOOLEAN :
					typeToValMap.put(sqlType, new Integer[] { Types.BOOLEAN });
					break;
				case CHAR :
					typeToValMap.put(sqlType, new Integer[] { Types.CHAR });
					break;
				case BYTE :
					typeToValMap.put(sqlType, new Integer[] { Types.TINYINT });
					break;
				case BYTE_ARRAY :
					typeToValMap.put(sqlType, new Integer[] { Types.VARBINARY });
					break;
				case SHORT :
					typeToValMap.put(sqlType, new Integer[] { Types.SMALLINT });
					break;
				case INTEGER :
					typeToValMap.put(sqlType, new Integer[] { Types.INTEGER });
					break;
				case LONG :
					typeToValMap.put(sqlType, new Integer[] { Types.BIGINT, Types.DECIMAL, Types.NUMERIC });
					break;
				case FLOAT :
					typeToValMap.put(sqlType, new Integer[] { Types.FLOAT });
					break;
				case DOUBLE :
					typeToValMap.put(sqlType, new Integer[] { Types.DOUBLE });
					break;
				case SERIALIZABLE :
					typeToValMap.put(sqlType, new Integer[] { Types.VARBINARY });
					break;
				case BLOB :
					// the following do not need to be handled except in specific situations
					typeToValMap.put(sqlType, new Integer[] { Types.BLOB });
					break;
				case UNKNOWN :
					typeToValMap.put(sqlType, new Integer[] {});
					break;
				default :
					throw new IllegalArgumentException("No JDBC mapping for unknown SqlType " + sqlType);
			}
		}
	}

	/**
	 * Returns the primary type value associated with the SqlType argument.
	 */
	public static int getTypeValForSqlType(SqlType sqlType) throws SQLException {
		Integer[] typeVals = typeToValMap.get(sqlType);
		if (typeVals.length == 0) {
			throw new SQLException("SqlType does not have any JDBC type value mapping: " + sqlType);
		}
		return typeVals[0];
	}
}
