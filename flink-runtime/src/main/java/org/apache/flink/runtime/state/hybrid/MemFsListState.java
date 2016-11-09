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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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

	private int maxTuplesInMemory;

	private BucketListShared bucketListShared = new BucketListShared();

	private Queue<QueueElement> readQueue = new ConcurrentLinkedQueue<>(),
		writeQueue = new ConcurrentLinkedQueue<>();

	private Map<String, Queue<String>> readResults = new ConcurrentHashMap<>();

	private Thread ioThread = new Thread() {
		@Override
		public void run() {

			try {
				Map<String, BufferedReader> readFiles = new HashMap<>();
				Map<String, PrintWriter> writeFiles = new HashMap<>();
				QueueElement element;
				while (true) {

					if (!readQueue.isEmpty()) {
						element = readQueue.poll();

						BufferedReader br = readFiles.get(element.getFName());
						if(br == null) {
							br = new BufferedReader(new FileReader(element.getFName()));
							readFiles.put(element.getFName(), br);
						}

						String value = br.readLine();
						Queue<String> results = readResults.get(element.getFName());
						if(results == null) {
							results = new ConcurrentLinkedQueue<>();
							readResults.put(element.getFName(), results);
						}
						results.add(value);

					} else if (!writeQueue.isEmpty()) {
						element = writeQueue.poll();

						PrintWriter pw = writeFiles.get(element.getFName());
						if (pw == null) {
							pw = new PrintWriter(new FileWriter(element.getFName()), true);
							writeFiles.put(element.getFName(), pw);
						}
						pw.println(element.getValue());

					} else {
						Thread.sleep(10);
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
		return currentNSState != null ?
			currentNSState.get(currentKey) : null;
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
			bucketList = new BucketList<>(maxTuplesInMemory, bucketListShared, readQueue, writeQueue, readResults);
			currentNSState.put(currentKey, bucketList);
		}

		bucketList.add(value);
	}

	public void purge() {
		BucketList<V> bucketList = (BucketList<V>) get();
		bucketList.purge();
	}

	public void clean() {
		BucketList<V> bucketList = (BucketList<V>) get();
		bucketList.clear();
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
}
