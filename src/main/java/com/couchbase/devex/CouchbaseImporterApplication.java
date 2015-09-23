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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point of the application. Injecting Couchbase configuration through the
 * {@link com.couchbase.devex.Database} object. The
 * {@link com.couchbase.devex.ImportJsonToCouchbase} object is writing any
 * Couchbase {@link com.couchbase.client.java.document.Document} instance to the
 * configured Couchbase Server.
 * 
 * Importers are injected depending on the configuration properties. All
 * importers must implement the @{link com.couchbase.devex.ImporterConfig}
 * interface. They must return an Observable of
 * {@link com.couchbase.client.java.document.Document}.
 * 
 * @author ldoguin
 */

@SpringBootApplication
@EnableConfigurationProperties
public class CouchbaseImporterApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(CouchbaseImporterApplication.class, args);
	}

	@Autowired
	private Database database;

	@Autowired
	private ImportJsonToCouchbase importJsonToCouchbase;

	@Autowired
	private ImporterConfig selectedConfig;

	@Override
	public void run(String... args) throws Exception {
		selectedConfig.startImport().flatMap(importJsonToCouchbase)
				.toBlocking().last();
	}

}
