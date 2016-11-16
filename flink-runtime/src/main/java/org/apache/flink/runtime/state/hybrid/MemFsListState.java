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

import java.io.BufferedReader;
import java.io.File;
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

	private static final int N_THREADS = 8;

	private int maxTuplesInMemory;

	private JSONSerializer serializer = new JSONSerializer();

	private BucketListShared bucketListShared = new BucketListShared();

	private Queue<QueueElement> readQueue = new ConcurrentLinkedQueue<>(), writeQueue = new ConcurrentLinkedQueue<>(),
		spillQueue = new ConcurrentLinkedQueue<>();

	private Map<String, Queue<String>> readResults = new ConcurrentHashMap<>();

	private Map<String, BucketList<V>> bucketLists = new ConcurrentHashMap<>();

	private Map<String, BufferedReader> readFiles = new HashMap<>();
	private Map<String, PrintWriter> writeFiles = new HashMap<>();

	private Queue<String> flushes = new ConcurrentLinkedQueue<>();

	private final Semaphore semaphoreStart = new Semaphore(N_THREADS);

	private final Semaphore semaphoreEnd = new Semaphore(N_THREADS);

	private Thread ioThread = new Thread() {
		@Override
		public void run() {

			try {
				QueueElement element;

				while (true) {

					if(!flushes.isEmpty()) {
						String id = flushes.poll();
						if(writeFiles.containsKey(id)) {
							writeFiles.get(id).flush();
						}

					} else if (!readQueue.isEmpty()) {
						element = readQueue.poll();

						Queue<String> results = readResults.get(element.getFName());

						BufferedReader br = readFiles.get(element.getFName());
						if(br == null) {
							System.out.println("opening file " + element.getFName());
							File f = new File(element.getFName());
							if(!f.exists()) {
								System.out.println("File does not exist: " + element.getFName());
								results.add("");
								continue;
							}
							br = new BufferedReader(new FileReader(f));
							readFiles.put(element.getFName(), br);
						}

//						if (results == null) {
//							results = new ConcurrentLinkedQueue<>();
//							readResults.put(element.getFName(), results);
//						}

						String value;
						while((value = br.readLine()) != null) {
							results.add(value);
						}
						results.add("");

					} else if (!writeQueue.isEmpty()) {
						element = writeQueue.poll();

						PrintWriter pw = writeFiles.get(element.getFName());
						if (pw == null) {
							System.out.println("creating file " + element.getFName());
							pw = new PrintWriter(new FileWriter(element.getFName()));
							writeFiles.put(element.getFName(), pw);
						}

						pw.println(element.getValue());
					} else if (!spillQueue.isEmpty()){
						semaphoreStart.release(N_THREADS);
						semaphoreEnd.acquire(N_THREADS);

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


	public MemFsListState(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, ListStateDescriptor<V> stateDesc, int maxTuplesInMemory) {
		super(keySerializer, namespaceSerializer, new ArrayListSerializer<>(stateDesc.getSerializer()), stateDesc);
		this.maxTuplesInMemory = maxTuplesInMemory;

		semaphoreStart.tryAcquire(N_THREADS);
		semaphoreEnd.tryAcquire(N_THREADS);

		ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);
		for(int i = 0; i < N_THREADS; i++) {
			executor.execute(new SpillThread());
		}
		ioThread.start();
	}

	public MemFsListState(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, ListStateDescriptor<V> stateDesc, HashMap<N, Map<K, ArrayList<V>>> state) {
		super(keySerializer, namespaceSerializer, new ArrayListSerializer<>(stateDesc.getSerializer()), stateDesc, state);
	}

	@Override
	public Iterable<V> get() {
		if (currentNSState == null) {
			currentNSState = state.get(currentNamespace);
		}

		if(currentNSState != null) {
			BucketList<V> result = (BucketList<V>) currentNSState.get(currentKey);
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
			bucketList = new BucketList<>(maxTuplesInMemory, bucketListShared, readQueue, writeQueue, spillQueue, readResults);
			bucketLists.put(bucketList.getSecondaryBucketFName(), bucketList);
			currentNSState.put(currentKey, bucketList);
		}

		bucketList.add(value);
	}

	public void purge() {
		BucketList<V> bucketList = (BucketList<V>) get();
		bucketList.purge();
	}

	public void clean() {
		System.out.println("clean called...");
		BucketList<V> bucketList = (BucketList<V>) get();
		bucketList.clear();
		String id = bucketList.getSecondaryBucketFName();
		bucketLists.remove(id);
		readFiles.remove(id);
		writeFiles.remove(id);
		readResults.remove(id);
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


	private class SpillThread implements Runnable {
		@Override
		public void run() {
			while (true) {

				try {
					semaphoreStart.acquire();

					QueueElement element = spillQueue.poll();
					if (element == null) {
						continue;
					}

					PrintWriter pw;
					synchronized (this) {
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

					BucketList<V> bucketList = bucketLists.get(element.getFName());
					if (bucketList != null) {

						BlockList<V> primaryBucket = bucketList.getPrimaryBucket();

						List<V> block;
						if (element.getBlockSize() == BucketList.BLOCK_SIZE) {
							block = primaryBucket.removeBlock();
						} else {
							block = primaryBucket.removeLastBlock();
						}

						StringBuilder sb = new StringBuilder();
						for (int i = 0; i < element.getBlockSize(); i++) {
							sb.append(serializer.serialize(block.remove(0)));
							sb.append('\n');
						}

						pw.print(sb.toString());

					}
				} catch(InterruptedException e) {
					e.printStackTrace();
				} finally {
					semaphoreEnd.release();
				}

			}
		}
	}
}
