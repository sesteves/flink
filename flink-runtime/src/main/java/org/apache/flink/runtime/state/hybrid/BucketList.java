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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO: ArrayList is extended here only to provide compatibility with AbstractHeapState.
 */
public class BucketList<V> extends ArrayList<V> implements Iterator<V>, Iterable<V> {

	private static final double PRIMARY_BUCKET_AFTER_FLUSH_FACTOR = 0.1;

	public static final int BLOCK_SIZE = 15000;

	// private List<V> primaryBucket;
	private BlockList<V> primaryBucket;

	private long primaryBucketSize;

	private long primaryBucketAfterFlushSize;

	private int primaryBucketIndex = 0;

//	private BufferedReader br;

	private V line;

	private V firstLine;

	private boolean first = true;

	private String secondaryBucketFName = "state/state-" + UUID.randomUUID().toString();

//	private PrintWriter secondaryBucket;

	private boolean usePrimaryBucket = true;

	private boolean abortSpilling = false;

	private ReentrantLock primaryBucketLock = new ReentrantLock();

	private BucketListShared bucketListShared;

	private boolean readingFromDisk = false;

	private Queue<QueueElement> readQueue, writeQueue, spillQueue;

	private Queue<V> readResults = new ConcurrentLinkedQueue<>();

	private boolean readRequested = false;

	private Queue<V> writeBuffer = new ConcurrentLinkedQueue<>();

	private boolean eof = false;

	public BucketList(int primaryBucketSize, BucketListShared bucketListShared, Queue<QueueElement> readQueue, Queue<QueueElement> writeQueue, Queue<QueueElement> spillQueue) {
//		primaryBucket = new ArrayList<>(primaryBucketSize);
		primaryBucket = new BlockList<>(primaryBucketSize, BLOCK_SIZE);

		this.primaryBucketSize = primaryBucketSize;
		primaryBucketAfterFlushSize = Math.round(PRIMARY_BUCKET_AFTER_FLUSH_FACTOR * primaryBucketSize);

		this.bucketListShared = bucketListShared;

		this.readQueue = readQueue;
		this.writeQueue = writeQueue;
		this.spillQueue = spillQueue;

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
			readRequested = false;
			eof = false;

			primaryBucketAfterFlushSize = -1;
			if(!primaryBucket.isEmpty()) {
				line = primaryBucket.get(0);
				primaryBucket.clear();
			}


			if (readingFromDisk) {
				bucketListShared.setFinalProcessing(false);
				readingFromDisk = false;
			}

			// spill disabled
//			if (!usePrimaryBucket) {
//				new Thread() {
//					@Override
//					public void run() {
//						// System.out.println("Before Primary Bucket Size: " + primaryBucket.size());
//						primaryBucketLock.lock();
//						if(!abortSpilling && primaryBucket.size() > primaryBucketAfterFlushSize) {
//
//							if (first) {
//								firstLine = primaryBucket.removeLast();
//								line = firstLine;
//								first = false;
//							}
//
//							long excess = primaryBucket.size() - primaryBucketAfterFlushSize;
//							long blocks = excess / BLOCK_SIZE;
//							long remaining = excess % BLOCK_SIZE;
//
//							System.out.println("spilling... excess: " + excess + ", blocks: " + blocks + ", remaining: " + remaining);
//
//							for (int i = 0; i < blocks; i++) {
//								// while (primaryBucket.size() > primaryBucketAfterFlushSize) {
//
//								if (abortSpilling) {
//									System.out.println("spilling aborted...");
//									break;
//								}
//
//
//								spillQueue.add(new QueueElement(secondaryBucketFName, BLOCK_SIZE));
//								//secondaryBucket.println(json);
//
//								// add(primaryBucket.remove(0));
//
//								// String json = serializer.serialize(value);
//
//							}
//							if (!abortSpilling && remaining != 0) {
//								spillQueue.add(new QueueElement(secondaryBucketFName, remaining));
//							}
//						} else {
//							System.out.println("spilling aborted...");
//						}
//						primaryBucketLock.unlock();
//
//					}
//				}.start();
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
			System.out.println("requesting read...");
			readQueue.add(new QueueElement(secondaryBucketFName));
			readRequested = true;
		}

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

			result = line;

			// collect result
			long startTick = System.currentTimeMillis();
			while(readResults.isEmpty() && !eof) {
				if(System.currentTimeMillis() - startTick > 2000) {
					System.out.println("taking to long to obtain results...");
					startTick = System.currentTimeMillis();
				}
			}

			line = readResults.poll();
		}

		return result;
	}

	@Override
	public boolean add(V value) {
		if ((usePrimaryBucket && primaryBucket.size() < primaryBucketSize) ||
			(!usePrimaryBucket && primaryBucket.size() < primaryBucketAfterFlushSize)) {
			primaryBucket.add(value);

			writeBuffer.add(value);
			writeQueue.add(new QueueElement(secondaryBucketFName));
		} else {
			if (first) {
				firstLine = value;
				line = firstLine;
				first = false;
			} else {
				writeBuffer.add(value);
				writeQueue.add(new QueueElement(secondaryBucketFName));
			}
		}


// spill disabled
//		else {
//			if (first) {
//				firstLine = value;
//				line = firstLine;
//				first = false;
//			} else {
//				writeBuffer.add(value);
//				writeQueue.add(new QueueElement(secondaryBucketFName));
//			}
//		}
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
		writeBuffer.clear();
		readResults.clear();
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

	public BlockList<V> getPrimaryBucket() {
		return primaryBucket;
	}

	public Queue<V> getWriteBuffer() {
		return writeBuffer;
	}

	public Queue<V> getReadResultsBuffer() {
		return readResults;
	}

	public void markEOF() {
		eof = true;
	}
}
