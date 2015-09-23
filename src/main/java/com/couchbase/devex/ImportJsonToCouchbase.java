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

import static com.couchbase.client.core.time.Delay.fixed;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import rx.Observable;
import rx.functions.Func1;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.util.retry.RetryBuilder;

/**
 * Created by ldoguin on 16/08/15.
 */
@Configuration
public class ImportJsonToCouchbase implements
		Func1<Document, Observable<? extends Document>> {

	private static final Log log = LogFactory
			.getLog(CouchbaseImporterApplication.class);

	@Value("${errorLogFilename:error.out}")
	private String errorLogFilename;

	@Value("${successLogFilename:success.out}")
	private String successLogFilename;

	@Value("${requestCancelledExceptionDelay:31000}")
	private long requestCancelledExceptionDelay;

	@Value("${requestCancelledExceptionRetries:100}")
	private int requestCancelledExceptionRetries;

	@Value("${temporaryFailureExceptionDelay:100}")
	private long temporaryFailureExceptionDelay;

	@Value("${temporaryFailureExceptionRetries:100}")
	private int temporaryFailureExceptionRetries;

	@Value("${importTimeout:500}")
	private long importTimeout;

	@Autowired
	AsyncBucket asyncBucket;

	public void writeToSuccessLog(String text) {
		writeToFile(successLogFilename, text);
	}

	public void writeToErrorLog(String text) {
		writeToFile(errorLogFilename, text);
	}

	public void writeToFile(String filename, String text) {
		try (FileWriter fw = new FileWriter(filename, true);) {
			fw.write(text + System.getProperty("line.separator"));
			fw.close();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

	@Override
	public Observable<? extends Document> call(Document doc) {
		return asyncBucket
				.upsert(doc)
				.timeout(importTimeout, TimeUnit.MILLISECONDS)
				.retryWhen(
						RetryBuilder
								.anyOf(RequestCancelledException.class)
								.delay(fixed(requestCancelledExceptionDelay,
										TimeUnit.MILLISECONDS))
								.max(requestCancelledExceptionRetries).build())
				.retryWhen(
						RetryBuilder
								.anyOf(TemporaryFailureException.class,
										BackpressureException.class)
								.delay(fixed(temporaryFailureExceptionDelay,
										TimeUnit.MILLISECONDS))
								.max(temporaryFailureExceptionRetries).build())
				.doOnError(t -> writeToErrorLog(doc.id()))
				.doOnNext(jd -> writeToSuccessLog(doc.id()))
				.onErrorResumeNext(
						new Func1<Throwable, Observable<Document>>() {
							@Override
							public Observable<Document> call(Throwable throwable) {
								log.error(String.format(
										"Could not import document ", doc.id()));
								log.error(throwable);
								return Observable.empty();
							}
						});
	}
}
