/*
 * Copyright 2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.devex;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import rx.Observable;

/**
 * Created by ldoguin on 17/08/15.
 */

@ConfigurationProperties("jdbc")
@Configuration
@ConditionalOnProperty(name = { "choosenImporter" }, havingValue = "JDBC")
public class JDBCConfig implements ImporterConfig {

	private static final Log log = LogFactory.getLog(JDBCConfig.class);

	public static final String SELECT_EVERYTHING_FROM_TABLE_QUERY = "Select * from %s;";

	@Autowired
	JdbcTemplate jdbcTemplate;

	private String tablesSchemaId = "myDatabaseSchema";
	private String catalog = null;
	private String schemaPattern = "public";
	private String tableNamePattern = null;
	private String[] types = null;
	private String columnNamePattern = null;

	@Override
	public Observable<Document> startImport() throws Exception {
		// get Database Medatadata objects to retrieve Tables schema
		DatabaseMetaData databaseMetadata = jdbcTemplate.getDataSource().getConnection().getMetaData();
		List<String> tableNames = new ArrayList<String>();
		// Get tables names
		ResultSet result = databaseMetadata.getTables(catalog, schemaPattern, tableNamePattern, types);
		while (result.next()) {
			String tablename = result.getString(3);
			String tableType = result.getString(4);
			// make sure we only import table(as oppose to Views, counter etc...)
			if (!tablename.isEmpty() && "TABLE".equals(tableType)) {
				tableNames.add(tablename);
				log.debug("Will import table " + tablename);
			}
		}
		// Map the tables schema to Table objects
		Map<String, Table> tables = new HashMap<String, Table>();
		JsonObject tablesSchema = JsonObject.create();
		for (String tableName : tableNames) {
			result = databaseMetadata.getColumns(catalog, schemaPattern, tableName, columnNamePattern);
			Table table = new Table(tableName);
			while (result.next()) {
				String columnName = result.getString(4);
				// Maps to JDBCType enum
				int columnType = result.getInt(5);
				table.addColumn(columnName, columnType);
			}
			result = databaseMetadata.getPrimaryKeys(catalog, schemaPattern, tableName);
			while (result.next()) {
				String columnName = result.getString(4);
				table.setPrimaryKey(columnName);
			}
			tables.put(tableName, table);
			tablesSchema.put(tableName, table.toJsonObject());
		}
		JsonDocument schemaDoc = JsonDocument.create(tablesSchemaId, tablesSchema);
		log.debug(tablesSchema);
		// FlatMap each table to an Observable of JsonDocument, one
		// JsonDocument per table row.
		return Observable.from(tableNames).flatMap(s -> {
			String sql = String.format(SELECT_EVERYTHING_FROM_TABLE_QUERY, s);
			return Observable.from(jdbcTemplate.query(sql, new JSONRowMapper(tables.get(s))));
		})
		// start by a jsonDocument containing the tables to be imported.
		.startWith(schemaDoc);
	}

	/**
	 * Map the {@link ResultSet} to a {@link JsonDocument} using the given
	 * {@link Table} definition. Most of the logic is in
	 * {@link JSONRowMapper#getJsonTypedValue(int, Object, String)} that returns
	 * an appropriate JSON type for a {@link JDBCType}.
	 * 
	 * @author ldoguin
	 *
	 */
	public class JSONRowMapper implements RowMapper<Document> {
		Table table;

		public JSONRowMapper(Table table) {
			this.table = table;
		}

		public JsonDocument mapRow(ResultSet rs, int rowNum) throws SQLException {
			String id = table.getName() + "::" + rs.getString(table.getPrimaryKey());
			JsonObject obj = JsonObject.create();
			for (Column col : table.getColumns()) {
				Object value = getJsonTypedValue(col.type, rs.getObject(col.name), col.name);
				obj.put(col.name, value);
			}
			return JsonDocument.create(id, obj);
		}

		public Object getJsonTypedValue(int type, Object value, String name) throws SQLException {
			if (value == null) {
				return null;
			}
			JDBCType current = JDBCType.valueOf(type);
			switch (current) {
			case TIMESTAMP:
				Timestamp timestamp = (Timestamp) value;
				return timestamp.getTime();
			case TIMESTAMP_WITH_TIMEZONE:
				Timestamp ts = (Timestamp) value;
				JsonObject tsWithTz = JsonObject.create();
				tsWithTz.put("timestamp", ts.getTime());
				tsWithTz.put("timezone", ts.getTimezoneOffset());
				return tsWithTz;
			case DATE:
				Date sqlDate = (Date) value;
				return sqlDate.getTime();
			case DECIMAL:
			case NUMERIC:
				BigDecimal bigDecimal = (BigDecimal) value;
				return bigDecimal.doubleValue();
			case ARRAY:
				Array array = (Array) value;
				Object[] objects = (Object[]) array.getArray();
				return JsonArray.from(objects);
			case BINARY:
			case BLOB:
			case LONGVARBINARY:
				return Base64.getEncoder().encodeToString((byte[]) value);
			case OTHER:
			case JAVA_OBJECT:
				// database specific, default to String value
				return value.toString();
			default:
				return value;
			}
		}
	}

	public class Table {

		String name;

		List<Column> columns = new ArrayList<Column>();

		String primaryKey;

		public Table() {
		}

		public Table(String tableName) {
			this.name = tableName;
		}

		public void setPrimaryKey(String primaryKey) {
			this.primaryKey = primaryKey;
		}

		public void addColumn(String name, int type) {
			columns.add(new Column(name, type));
		}

		public String getName() {
			return name;
		}

		public List<Column> getColumns() {
			return columns;
		}

		public String getPrimaryKey() {
			return primaryKey;
		}

		public JsonObject toJsonObject() {
			JsonObject obj = JsonObject.create();
			JsonArray jsonColumns = JsonArray.create();
			for (Column col : columns) {
				jsonColumns.add(col.toJsonObject());
			}
			obj.put("tableName", name);
			obj.put("primaryKey", primaryKey);
			obj.put("columns", jsonColumns);
			return obj;
		}
	}

	public class Column {

		String name;

		int type;

		public Column() {
		}

		public Column(String name, int type) {
			this.name = name;
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public int getType() {
			return type;
		}

		public JsonObject toJsonObject() {
			JsonObject obj = JsonObject.create();
			obj.put("name", name);
			obj.put("type", type);
			return obj;
		}

	}
}
