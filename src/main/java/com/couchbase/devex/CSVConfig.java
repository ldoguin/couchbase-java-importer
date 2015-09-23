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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import rx.Observable;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

/**
 * This is a CSV importer configuration. To select it, set the
 * 'choosenImporter' property to 'CSV'. 
 * 
 * @author ldoguin
 */
@ConfigurationProperties("csv")
@Configuration
@ConditionalOnProperty(name = { "choosenImporter" }, havingValue = "CSV")
public class CSVConfig implements ImporterConfig {

	public enum RowType {
		STRING, LONG, DOUBLE, BOOLEAN, DATE;

		public void addField(ObjectNode node, String name, String value,
				SimpleDateFormat sdf) {
			switch (this) {
			case STRING:
				node.put(name, value);
				break;

			case LONG:
				long l = Long.valueOf(value.replaceAll("\\u00A0", ""));
				node.put(name, l);
				break;

			case DOUBLE:
				value = value.replaceAll("\\u00A0", "");
				double d = Double.valueOf(value);
				node.put(name, d);
				break;

			case BOOLEAN:
				boolean b = Boolean.valueOf(value);
				node.put(name, b);
				break;

			case DATE:
				Calendar c = Calendar.getInstance();
				try {
					c.setTime(sdf.parse(value));
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
				long dl = c.getTimeInMillis();
				node.put(name, dl);
				break;
			}

		}

		@Override
		public String toString() {
			return super.toString();
		}
	}

	private List<String> columName = new ArrayList<String>();

	private List<String> columType = new ArrayList<String>();

	private char quoteChar;

	private char columnSeparator;

	private Boolean skipFirstLineForNames;

	private String csvFilePath;

	private int keyColumIndex = 0;

	private int totalcolumns = 0;

	private String dateFormat = "EEE MMM dd HH:mm:ss z yyyy";

	private String languageTag = "EN_US";

	private String keyPrefix = "";

	private SimpleDateFormat sdf;

	private ObjectMapper objectMapper = new ObjectMapper();

	public List<String> getColumName() {
		return columName;
	}

	public void setColumName(List<String> columName) {
		this.columName = columName;
	}

	public List<String> getColumType() {
		return columType;
	}

	public void setColumType(List<String> columType) {
		this.columType = columType;
	}

	public char getQuoteChar() {
		return quoteChar;
	}

	public void setQuoteChar(char quoteChar) {
		this.quoteChar = quoteChar;
	}

	public char getColumnSeparator() {
		return columnSeparator;
	}

	public void setColumnSeparator(char columnSeparator) {
		this.columnSeparator = columnSeparator;
	}

	public String getCsvFilePath() {
		return csvFilePath;
	}

	public void setCsvFilePath(String csvFilePath) {
		this.csvFilePath = csvFilePath;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getLanguageTag() {
		return languageTag;
	}

	public void setLanguageTag(String languageTag) {
		this.languageTag = languageTag;
	}

	public String getKeyPrefix() {
		return keyPrefix;
	}

	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}

	public SimpleDateFormat getSimpleDateFormat() {
		if (sdf == null) {
			sdf = new SimpleDateFormat(getDateFormat(),
					Locale.forLanguageTag(languageTag));
		}
		return sdf;
	}

	public int getTotalColumn() {
		if (totalcolumns > 0) {
			return totalcolumns;
		}
		return columType.size();
	}

	public Boolean getSkipFirstLineForNames() {
		return skipFirstLineForNames;
	}

	public void setSkipFirstLineForNames(Boolean skipFirstLineForNames) {
		this.skipFirstLineForNames = skipFirstLineForNames;
	}

	public String getKeyColum() {
		return getColumName().get(keyColumIndex);
	}

	public void setKeyColumIndex(int keyColumIndex) {
		this.keyColumIndex = keyColumIndex;
	}

	public Observable<Document> createNode(String[] line) {
		ObjectNode node = objectMapper.createObjectNode();
		for (int i = 0; i < getTotalColumn(); i++) {
			String columnType = getColumType().get(i);
			RowType.valueOf(columnType).addField(node, columName.get(i),
					line[i], getSimpleDateFormat());
		}
		String kc = getKeyColum();
		String key = getKeyPrefix() + node.get(kc).asText();
		RawJsonDocument rjd = RawJsonDocument.create(key, node.toString());
		return Observable.just(rjd);
	}

	@Override
	public Observable<Document> startImport() {
		FileInputStream csvFile;
		try {
			csvFile = new FileInputStream(getCsvFilePath());
			CsvMapper mapper = new CsvMapper();
			mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
			CsvSchema csvSchema = CsvSchema.emptySchema()
					.withColumnSeparator(getColumnSeparator())
					.withQuoteChar(getQuoteChar());
			ObjectReader reader = mapper.reader(String[].class);
			MappingIterator<String[]> it = reader.with(csvSchema).readValues(
					csvFile);
			if (!getSkipFirstLineForNames()) {
				String[] firstline = it.next();
				updateColumnNames(firstline);
			}
			return Observable.from(new Iterable<String[]>() {
				@Override
				public Iterator<String[]> iterator() {
					return it;
				}
			}).flatMap(line -> createNode(line));
		} catch (FileNotFoundException e) {
			return Observable.error(e);
		} catch (IOException e) {
			return Observable.error(e);
		}
	}

	private void updateColumnNames(String[] line) {
		for (int i = 0; i < getTotalColumn(); i++) {
			columName.add(line[i]);
		}
		;
	}

}
