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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO: ArrayList is extended here only to provide compatibility with AbstractHeapState.
 */
public class BucketList<V> extends ArrayList<V> implements Iterator<V>, Iterable<V> {

	private static final double PRIMARY_BUCKET_AFTER_FLUSH_FACTOR = 0.1;

	private List<V> primaryBucket;

	private long primaryBucketSize;

	private long primaryBucketAfterFlushSize;

	private int primaryBucketIndex = 0;

//	private BufferedReader br;

	private String line;

	private String firstLine;

	private boolean first = true;

	private String secondaryBucketFName = "state/state-" + UUID.randomUUID().toString();

//	private PrintWriter secondaryBucket;

	JSONSerializer serializer = new JSONSerializer();

	private JSONDeserializer deserializer = new JSONDeserializer().use(Tuple2.class, new TupleObjectFactory());

	private boolean usePrimaryBucket = true;

	private boolean abortSpilling = false;

	private ReentrantLock primaryBucketLock = new ReentrantLock();

	private BucketListShared bucketListShared;

	private boolean readingFromDisk = false;

	private boolean flush = true;

	private Queue<QueueElement> readQueue, writeQueue;

	private Queue<String> readResults = new ConcurrentLinkedQueue<>();

	private boolean readRequested = false;

	public BucketList(int primaryBucketSize, BucketListShared bucketListShared, Queue<QueueElement> readQueue, Queue<QueueElement> writeQueue, Map<String, Queue<String>> readResults) {
		primaryBucket = new ArrayList<>(primaryBucketSize);
		this.primaryBucketSize = primaryBucketSize;
		primaryBucketAfterFlushSize = Math.round(PRIMARY_BUCKET_AFTER_FLUSH_FACTOR * primaryBucketSize);

		this.bucketListShared = bucketListShared;

		this.readQueue = readQueue;
		this.writeQueue = writeQueue;
		readResults.put(secondaryBucketFName, this.readResults);

//		buffer = new ArrayList<>(primaryBucketSize);

//		try {
			// create file
//			secondaryBucket = new PrintWriter(new FileWriter(secondaryBucketFName));
//			secondaryBucket.close();

//			br = new BufferedReader(new FileReader(secondaryBucketFName));
//			stats = new PrintWriter(new FileOutputStream(new File("stats.txt"), true));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}

	@Override
	public boolean hasNext() {
		if (primaryBucketIndex < primaryBucket.size() || line != null) {
			return true;
		} else {
			System.out.println("hasNext called for " + secondaryBucketFName);

			primaryBucketIndex = 0;
			abortSpilling = false;
			flush = true;
			readRequested = false;

			if (readingFromDisk) {
				bucketListShared.setFinalProcessing(false);
				readingFromDisk = false;
			}

			if (!usePrimaryBucket) {

				new Thread() {
					@Override
					public void run() {
						// System.out.println("Before Primary Bucket Size: " + primaryBucket.size());
						primaryBucketLock.lock();
						if(!abortSpilling && primaryBucket.size() > primaryBucketAfterFlushSize) {

							if (first) {
								firstLine = serializer.serialize(primaryBucket.remove(0));
								line = firstLine;
								first = false;
							}

							long blockSize = 10000;
							long excess = primaryBucket.size() - primaryBucketAfterFlushSize;
							long blocks = excess / blockSize;

							System.out.println("spilling... excess: " + excess + ", blocks: " + blocks + ", remaining: " + (excess % blockSize));

							for (int i = 0; i < blocks; i++) {
								// while (primaryBucket.size() > primaryBucketAfterFlushSize) {

								if (abortSpilling) {
									// TODO flush string builder
									System.out.println("spilling aborted...");
									break;
								}


								writeQueue.add(new QueueElement(secondaryBucketFName, blockSize));
								//secondaryBucket.println(json);

								// add(primaryBucket.remove(0));

								// TODO make more efficient add
								// String json = serializer.serialize(value);

							}
							if (!abortSpilling && excess % blockSize != 0) {
								writeQueue.add(new QueueElement(secondaryBucketFName, excess % blockSize));
							}
						} else {
							System.out.println("spilling aborted...");
						}
						primaryBucketLock.unlock();

					}
				}.start();
			}

//			try {
//				br.close();
//				br = new BufferedReader(new FileReader(secondaryBucketFName));
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
			if (firstLine != null) {
				line = firstLine;
			}

			return false;
		}
	}

	@Override
	public V next() {
		V result = null;

		if(line != null && !readRequested) {
			readQueue.add(new QueueElement(secondaryBucketFName, null));
			readRequested = true;
		}


//		if (flush) {
//			flush = false;
//			new Thread() {
//				@Override
//				public void run() {
//					secondaryBucket.flush();
//				}
//			}.start();
//		}

		if (primaryBucketIndex < primaryBucket.size()) {
			if (!usePrimaryBucket) {
				abortSpilling = true;
			}

			primaryBucketLock.lock();
			result = primaryBucket.get(primaryBucketIndex++);
			primaryBucketLock.unlock();

		} else if (line != null) {
			readingFromDisk = true;
			bucketListShared.setFinalProcessing(true);

			result = (V) deserializer.deserialize(line);

			// collect result
			long startTick = System.currentTimeMillis();
			while(readResults.isEmpty()) {
				if(System.currentTimeMillis() - startTick > 2000) {
					System.out.println("taking to long to obtain results...");
					startTick = System.currentTimeMillis();
				}
			}
			String value = readResults.poll();
			line = "".equals(value) ? null : value;

		}

		return result;
	}

	@Override
	public boolean add(V value) {
		if ((usePrimaryBucket && primaryBucket.size() < primaryBucketSize) ||
			(!usePrimaryBucket && primaryBucket.size() < primaryBucketAfterFlushSize)) {
			primaryBucket.add(value);
		} else {
			String json = serializer.serialize(value);
			if (first) {
				firstLine = json;
				line = firstLine;
				first = false;
			} else {
				writeQueue.add(new QueueElement(secondaryBucketFName, json));
				//secondaryBucket.println(json);
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

	public String getSecondaryBucketFName() {
		return secondaryBucketFName;
	}

	public List<V> getPrimaryBucket() {
		return primaryBucket;
	}

	public Lock getPrimaryBucketLock() {
		return primaryBucketLock;
	}
}
