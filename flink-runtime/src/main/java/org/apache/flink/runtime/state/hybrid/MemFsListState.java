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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.memory.AbstractMemState;
import org.apache.flink.runtime.state.memory.AbstractMemStateSnapshot;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ListState} that is snapshotted
 * into a serialized memory copy.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
public class MemFsListState<K, N, V>
	extends AbstractMemState<K, N, ArrayList<V>, ListState<V>, ListStateDescriptor<V>>
	implements ListState<V> {

	// TODO get this size when records are written
	private static final int LINE_LENGTH = 2292;

	private int maxTuplesInMemory;

	private JSONSerializer serializer = new JSONSerializer();

	private JSONDeserializer deserializer = new JSONDeserializer().use(Tuple2.class, new TupleObjectFactory());

	private BucketListShared bucketListShared = new BucketListShared();

	private Queue<QueueElement> readQueue = new ConcurrentLinkedQueue<>(), writeQueue = new ConcurrentLinkedQueue<>(),
		spillQueue = new ConcurrentLinkedQueue<>();

	private Map<String, BucketList<V>> bucketLists = new ConcurrentHashMap<>();

	private Map<String, BufferedReader> readFiles = new HashMap<>();
	private Map<String, PrintWriter> writeFiles = new HashMap<>();

	private Queue<String> flushes = new ConcurrentLinkedQueue<>();

	private final Semaphore semaphoreSpillStart, semaphoreSpillEnd;

	private final Semaphore semaphoreReadStart, semaphoreReadEnd;

	private boolean spill;

	private double tuplesAfterSpillFactor;

	private int spillThreads;

	private BucketList<V> bucketListToRead;

	private Thread ioThread = new Thread() {
		@Override
		public void run() {

			try {
				QueueElement element;

				while (true) {

					if(!flushes.isEmpty()) {
						String id = flushes.poll();
						PrintWriter pw = writeFiles.get(id);
						if(pw != null) {
							pw.flush();
						}

					} else if (!readQueue.isEmpty()) {
						element = readQueue.poll();

						bucketListToRead = bucketLists.get(element.getFName());

						// this flush is necessary for when there is a single past window and a spill does not
						// does not fit in 1 window duration
						PrintWriter pw = writeFiles.get(element.getFName());
						if(pw != null) {
							pw.flush();
							System.out.println("Releasing read threads...");
							semaphoreReadStart.release(spillThreads);
							semaphoreReadEnd.acquire(spillThreads);
							System.out.println("Done read threads...");
						}
						if(bucketListToRead != null) {
							System.out.println("Mark EOF...");
							bucketListToRead.markEOF();
						}

					} else if (!writeQueue.isEmpty()) {
						element = writeQueue.poll();

						BucketList<V> bucketList = bucketLists.get(element.getFName());
						if(bucketList != null) {
							Queue<V> writeBuffer = bucketList.getWriteBuffer();

							if (!writeBuffer.isEmpty()) {

								if (readFiles.get(element.getFName()) != null) {
									// if we have started prefecthing, then write to result buffer directly
									Queue<V> results = bucketList.getReadResultsBuffer();
									results.add(writeBuffer.poll());
								} else {
									PrintWriter pw = writeFiles.get(element.getFName());
									if (pw == null) {
										System.out.println("creating file " + element.getFName());
										pw = new PrintWriter(new FileWriter(element.getFName()));
										writeFiles.put(element.getFName(), pw);
									}

									for (int i = 0; i < BucketList.BLOCK_SIZE && !writeBuffer.isEmpty(); i++) {
										pw.println(serializer.serialize(writeBuffer.poll()));
									}
								}
							}
						}
					} else if (!spillQueue.isEmpty()){
						semaphoreSpillStart.release(spillThreads);
						semaphoreSpillEnd.acquire(spillThreads);

					} else {
						Thread.sleep(0);
					}

				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	};


	public MemFsListState(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<V> stateDesc, int maxTuplesInMemory, double tuplesAfterSpillFactor, int spillThreads) {
		super(keySerializer, namespaceSerializer, new ArrayListSerializer<>(stateDesc.getSerializer()), stateDesc);
		this.maxTuplesInMemory = maxTuplesInMemory;
		this.tuplesAfterSpillFactor = tuplesAfterSpillFactor;
		this.spillThreads = spillThreads;

		semaphoreSpillStart = new Semaphore(spillThreads);
		semaphoreSpillEnd = new Semaphore(spillThreads);
		semaphoreSpillStart.tryAcquire(spillThreads);
		semaphoreSpillEnd.tryAcquire(spillThreads);

		ExecutorService executorSpill = Executors.newFixedThreadPool(spillThreads);
		for(int i = 0; i < spillThreads; i++) {
			executorSpill.execute(new SpillThread());
		}

		// TODO change name of spill threads
		semaphoreReadStart = new Semaphore(spillThreads);
		semaphoreReadEnd = new Semaphore(spillThreads);
		semaphoreReadStart.tryAcquire(spillThreads);
		semaphoreReadEnd.tryAcquire(spillThreads);

		ExecutorService executorRead = Executors.newFixedThreadPool(spillThreads);
		for(int i = 0; i < spillThreads; i++) {
			executorRead.execute(new ReadThread());
		}

		ioThread.start();
	}

	public MemFsListState(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, ListStateDescriptor<V> stateDesc, HashMap<N, Map<K, ArrayList<V>>> state) {
		super(keySerializer, namespaceSerializer, new ArrayListSerializer<>(stateDesc.getSerializer()), stateDesc, state);

		semaphoreSpillStart = null;
		semaphoreSpillEnd = null;
		semaphoreReadStart = null;
		semaphoreReadEnd = null;
	}

	@Override
	public Iterable<V> get() {
		if (currentNSState == null) {
			currentNSState = state.get(currentNamespace);
		}

		if(currentNSState != null) {
			BucketList<V> result = (BucketList<V>) currentNSState.get(currentKey);
			System.out.println("get called... Flushing file: " + result.getSecondaryBucketFName());
			// flush
			flushes.add(result.getSecondaryBucketFName());
			return result;
		}
		return null;
	}

	@Override
	public void add(V value) {
		if (currentKey == null) {
			throw new RuntimeException("No key available.");
		}

		if (currentNSState == null) {
			currentNSState = new HashMap<>();
			state.put(currentNamespace, currentNSState);
		}


		BucketList<V> bucketList = (BucketList<V>) currentNSState.get(currentKey);
		if (bucketList == null) {
			bucketList = new BucketList<>(maxTuplesInMemory, bucketListShared, readQueue, writeQueue, spillQueue, tuplesAfterSpillFactor, false);
			bucketLists.put(bucketList.getSecondaryBucketFName(), bucketList);
			currentNSState.put(currentKey, bucketList);
//			spill = !spill;
		}

		bucketList.add(value);
	}

	public void prefetch() {
		BucketList<V> bucketList = (BucketList<V>) get();
		readQueue.add(new QueueElement(bucketList.getSecondaryBucketFName()));
		bucketList.setReadRequested();

		System.out.println("Prefetching... " + bucketList.getSecondaryBucketFName());
	}

	public void purge() {
		BucketList<V> bucketList = (BucketList<V>) get();
		bucketList.purge();
	}

	public void clean() {
		System.out.println("clean called...");
		BucketList<V> bucketList = (BucketList<V>) get();
		String id = bucketList.getSecondaryBucketFName();
		bucketLists.remove(id);
		bucketList.clear();
		readFiles.remove(id);
		writeFiles.remove(id);
	}

	@Override
	public KvStateSnapshot<K, N, ListState<V>, ListStateDescriptor<V>, MemoryStateBackend> createHeapSnapshot(byte[] bytes) {
		return new Snapshot<>(getKeySerializer(), getNamespaceSerializer(), stateSerializer, stateDesc, bytes);
	}

	public static class Snapshot<K, N, V> extends AbstractMemStateSnapshot<K, N, ArrayList<V>, ListState<V>, ListStateDescriptor<V>> {
		private static final long serialVersionUID = 1L;

		public Snapshot(TypeSerializer<K> keySerializer,
						TypeSerializer<N> namespaceSerializer,
						TypeSerializer<ArrayList<V>> stateSerializer,
						ListStateDescriptor<V> stateDescs, byte[] data) {
			super(keySerializer, namespaceSerializer, stateSerializer, stateDescs, data);
		}

		@Override
		public KvState<K, N, ListState<V>, ListStateDescriptor<V>, MemoryStateBackend> createMemState(HashMap<N, Map<K, ArrayList<V>>> stateMap) {
			return new MemFsListState<>(keySerializer, namespaceSerializer, stateDesc, stateMap);
		}
	}

	private Object getInstance() {
		return this;
	}

	private class SpillThread implements Runnable {
		@Override
		public void run() {
			while (true) {

				try {
					semaphoreSpillStart.acquire();

					QueueElement element = spillQueue.poll();
					if (element == null) {
						continue;
					}
					BucketList<V> bucketList = bucketLists.get(element.getFName());
					if (bucketList == null || bucketList.getEOF()) {
						continue;
					}

					PrintWriter pw;
					synchronized (getInstance()) {
						pw = writeFiles.get(element.getFName());
						if (pw == null) {
							System.out.println("creating file " + element.getFName());
							try {
								pw = new PrintWriter(new FileWriter(element.getFName()));
							} catch (IOException e) {
								e.printStackTrace();
							}
							writeFiles.put(element.getFName(), pw);
						}
					}

					BlockList<V> primaryBucket = bucketList.getPrimaryBucket();

					StringBuilder sb = new StringBuilder();
					List<V> block;
					if (element.getBlockSize() == BucketList.BLOCK_SIZE) {
						block = primaryBucket.removeBlock();

						for (int i = 0; i < element.getBlockSize(); i++) {
							sb.append(serializer.serialize(block.get(i)));
							sb.append('\n');
						}
					} else {
						int lastBlockSize = primaryBucket.getLastBlockSize();
						if (element.getBlockSize() >= lastBlockSize) {
							block = primaryBucket.removeLastBlock();

							for (int i = 0; i < lastBlockSize; i++) {
								sb.append(serializer.serialize(block.get(i)));
								sb.append('\n');
							}

							if (element.getBlockSize() > lastBlockSize) {
								int remaining = element.getBlockSize() - lastBlockSize;

								for (int i = 0; i < remaining; i++) {
									sb.append(serializer.serialize(primaryBucket.removeLast()));
									sb.append('\n');
								}
							}
						} else {
							for (int i = 0; i < element.getBlockSize(); i++) {
								sb.append(serializer.serialize(primaryBucket.removeLast()));
								sb.append('\n');
							}
						}
					}

					pw.print(sb.toString());

					System.out.println("spilled block.. pBucket size: " + primaryBucket.size());
				} catch(InterruptedException e) {
					e.printStackTrace();
				} finally {
					semaphoreSpillEnd.release();
				}

			}
		}
	}

	private class ReadThread implements Runnable {
		@Override
		public void run() {
			while (true) {

				try {
					semaphoreReadStart.acquire();

					// read remaining elements that were not written to disk
					Queue<V> writeBuffer = bucketListToRead.getWriteBuffer();
					Queue<V> results = bucketListToRead.getReadResultsBuffer();
					while(!writeBuffer.isEmpty()) {
						// to avoid locks we catch the exception
						try {
							results.add(writeBuffer.poll());
						} catch (NullPointerException e) {
							break;
						}
					}

					BufferedReader br;
					synchronized (getInstance()) {
						String fname = bucketListToRead.getSecondaryBucketFName();
						br = readFiles.get(fname);
						if (br == null) {
							System.out.println("opening file " + fname);
							File f = new File(fname);
							if (!f.exists()) {
								System.out.println("File does not exist: " + fname);
								bucketListToRead.markEOF();
								continue;
							}
							br = new BufferedReader(new FileReader(f), LINE_LENGTH * BucketList.BLOCK_SIZE);
							readFiles.put(fname, br);
						}
					}

					String value;
					while ((value = br.readLine()) != null) {
						results.add((V) deserializer.deserialize(value));
					}

				} catch(InterruptedException e) {
					e.printStackTrace();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					System.out.println("semaphore released " + this);
					semaphoreReadEnd.release();
				}

			}
		}
	}
}
