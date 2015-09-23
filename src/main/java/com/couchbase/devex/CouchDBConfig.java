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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import rx.Observable;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;

/**
 * Created by ldoguin on 17/08/15.
 */

@ConfigurationProperties("couchdb")
@Configuration
@ConditionalOnProperty(name = { "choosenImporter" }, havingValue = "COUCHDB")
public class CouchDBConfig implements ImporterConfig {

	private static final Log log = LogFactory.getLog(CouchDBConfig.class);

	public static final String TOTAL_ROWS_PROPERTY = "total_rows";

	public static final String OFFSET_PROPERTY = "offset";

	@Value("${couchdb.downloadURL:http://127.0.0.1:5984/database_export/_all_docs?include_docs=true}")
	String couchDBRequest;

	ObjectMapper om = new ObjectMapper();

	@Override
	public Observable<Document> startImport() {
		BufferedReader inp2 = null;
		try {
			URL url = new URL(couchDBRequest);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			// assume this is going to be a big file...
			conn.setReadTimeout(0);
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}

			inp2 = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		Observable.from(inp2.lines()::iterator).flatMap(s -> {
			JsonNode node = null;
			if (s.endsWith("\"rows\":[")) {
				// first line, find total rows, offset
				s = s.concat("]}");
				try {
					node = om.readTree(s.toString());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				long totalRows = node.get(TOTAL_ROWS_PROPERTY).asLong();
				long offset = node.get(OFFSET_PROPERTY).asLong();
				log.info(String.format(
						"Query starting at offset %d for a total of %d rows.",
						offset, totalRows));
				return Observable.empty();
			} else if (s.length() == 2) {
				// last line, do nothing
				log.info("end of the feed.");
				return Observable.empty();
			} else {
				try {
					if (s.endsWith(",")) {
						node = om.readTree(s.substring(0, s.length() - 1)
								.toString());
					} else {
						node = om.readTree(s.toString());
					}
					String key = node.get("key").asText();
					String jsonDoc = node.get("doc").toString();
					Document doc = RawJsonDocument.create(key, jsonDoc);
					return Observable.just(doc);
				} catch (IOException e) {
					return Observable.error(e);
				}
			}

		});
		return Observable.empty();
	}
}
