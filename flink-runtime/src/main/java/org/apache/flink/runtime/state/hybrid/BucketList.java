/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.hybrid;

import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * TODO: ArrayList is extended here only to provide compatibility with AbstractHeapState.
 */
public class BucketList<V> extends ArrayList<V> implements Iterator<V>, Iterable<V> {

	private static final double PRIMARY_BUCKET_AFTER_FLUSH_FACTOR = 0.1;

	private List<V> primaryBucket;

	private long primaryBucketSize;

	private long primaryBucketAfterFlushSize;

	private int primaryBucketIndex = 0;

	private BufferedReader br;

	private String line;

	private String firstLine;

	private boolean first = true;

	private String secondaryBucketFName = "state/state-" + UUID.randomUUID().toString();

	private PrintWriter secondaryBucket;

	JSONSerializer serializer = new JSONSerializer();

	private JSONDeserializer deserializer = new JSONDeserializer().use(Tuple2.class, new TupleObjectFactory());

//	private long startTick = 0, endTick = 0;

//	private PrintWriter stats;

	private boolean usePrimaryBucket = true;

	private boolean abortSpilling = false;

	private ReentrantLock primaryBucketLock = new ReentrantLock();

	public BucketList(int primaryBucketSize) {
		primaryBucket = new ArrayList<>(primaryBucketSize);
		this.primaryBucketSize = primaryBucketSize;
		primaryBucketAfterFlushSize = Math.round(PRIMARY_BUCKET_AFTER_FLUSH_FACTOR * primaryBucketSize);

		try {
			// autoflush set to true
			secondaryBucket = new PrintWriter(new FileWriter(secondaryBucketFName), true);

			br = new BufferedReader(new FileReader(secondaryBucketFName));
//			stats = new PrintWriter(new FileOutputStream(new File("stats.txt"), true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		if((primaryBucketIndex < primaryBucket.size()) || line != null) {
			return true;
		} else {

			primaryBucketIndex = 0;
			abortSpilling = false;

//			endTick = System.currentTimeMillis();
//			stats.println(endTick - startTick);
//			stats.close();
//			startTick = 0;
//			endTick = 0;

			if(!usePrimaryBucket && primaryBucket.size() > primaryBucketAfterFlushSize) {

				new Thread() {
					@Override
					public void run() {
						// System.out.println("Before Primary Bucket Size: " + primaryBucket.size());
						primaryBucketLock.lock();

						while (primaryBucket.size() > primaryBucketAfterFlushSize) {
							add(primaryBucket.remove(0));
							if(abortSpilling) {
								break;
							}
						}
						primaryBucketLock.unlock();

						// System.out.println("After Primary Bucket Size: " + primaryBucket.size());

					}
				}.start();
			}

			try {
				br.close();
				br = new BufferedReader(new FileReader(secondaryBucketFName));
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(firstLine != null) {
				line = firstLine;
			}

			return false;
		}
	}

	@Override
	public V next() {
		V result = null;
		if(primaryBucketIndex < primaryBucket.size()) {
			if(!usePrimaryBucket) {
				abortSpilling = true;
			}

//			if(startTick == 0) {
//				startTick = System.currentTimeMillis();
//			}
			primaryBucketLock.lock();
			result = primaryBucket.get(primaryBucketIndex++);
			primaryBucketLock.unlock();

		} else if(line != null) {
//			if(endTick == 0) {
//				endTick = System.currentTimeMillis();
//				stats.print(endTick - startTick + ",");
//				startTick = System.currentTimeMillis();
//			}

			result = (V)deserializer.deserialize(line);
			try {
				line = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return result;
	}

	@Override
	public boolean add(V value) {
		if((usePrimaryBucket && primaryBucket.size() < primaryBucketSize) ||
			(!usePrimaryBucket && primaryBucket.size() < primaryBucketAfterFlushSize)) {
			primaryBucket.add(value);
		} else {
			String json = serializer.serialize(value);
			if(first) {
				firstLine = json;
				line = firstLine;
				first = false;
			} else {
				secondaryBucket.println(json);
			}
		}
		return true;
	}

	public void purge() {
		usePrimaryBucket = false;
	}

	public void clear() {
//		System.out.println("### bucketList clear");
		abortSpilling = true;
		primaryBucketLock.lock();
		primaryBucket.clear();
		primaryBucketLock.unlock();
		// usePrimaryBucket = true;
	}

	@Override
	public void remove() {
	}

	@Override
	public Iterator iterator() {
		return this;
	}
}
