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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import rx.Observable;
import rx.functions.Func1;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.mongodb.rx.client.MongoClient;
import com.mongodb.rx.client.MongoClients;
import com.mongodb.rx.client.MongoDatabase;

/**
 * This is a MongoDB importer configuration. To select it, set the
 * 'choosenImporter' property to 'MONGODB'.
 * 
 * @author ldoguin
 */

@ConfigurationProperties("mongodb")
@Configuration
@ConditionalOnProperty(name = { "choosenImporter" }, havingValue = "MONGODB")
public class MongoDBConfig implements ImporterConfig {

	private static final Log log = LogFactory.getLog(MongoDBConfig.class);

	@Value("${mongodb.connectionString:mongodb://127.0.0.1:27017/}")
	String connectionString;

	@Value("${mongodb.dbName:test}")
	String dbName;

	@Value("${mongodb.collectionName:restaurants}")
	String collectionName;

	@Value("${mongodb.typeField:type}")
	String typeField;

	@Value("${mongodb.type:restaurant}")
	String type;

	@Override
	public Observable<Document> startImport() {
		MongoClient client = MongoClients.create(connectionString);
		MongoDatabase db = client.getDatabase(dbName);
		return db.getCollection(collectionName).find().toObservable()
				.map(new Func1<org.bson.Document, Document>() {
					public Document call(org.bson.Document mongoDoc) {
						mongoDoc.put(typeField, type);
						RawJsonDocument d = RawJsonDocument.create(mongoDoc
								.getObjectId("_id").toHexString(), mongoDoc
								.toJson());
						return d;
					};
				});
	}
}
