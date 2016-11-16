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
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class BlockList<T> {

	private List<List<T>> blockList;

	private int blockSize;

	private int size = 0;

	public BlockList(int capacity, int blockSize) {
		this.blockSize = blockSize;

		int blocks = capacity / blockSize;
		int remaining = capacity % blockSize;

		blockList = Collections.synchronizedList(new ArrayList<List<T>>(blocks + (remaining == 0? 0 : 1)));

		for(int i = 0; i < blocks; i++) {
			blockList.add(Collections.synchronizedList(new ArrayList<T>(blockSize)));
		}
		if(remaining != 0) {
			blockList.add(Collections.synchronizedList(new ArrayList<T>(remaining)));
		}
	}

	public void add(T object) {
		int lastBlock = size / blockSize;
		blockList.get(lastBlock).add(object);
		size++;
	}

	public T get(int index) {
		int block = index / blockSize;
		int remaining = index % blockSize;

		return blockList.get(block).get(remaining);
	}

	public synchronized List<T> removeBlock() {
		List<T> list = blockList.remove(0);
		size -= list.size();
		return list;
	}

	public synchronized List<T> removeLastBlock() {
		int lastBlock = size / blockSize;
		if(size % blockSize == 0) {
			lastBlock--;
		}
		List<T> list = blockList.remove(lastBlock);
		size -= list.size();
		return list;
	}

	public T removeLast() {
		int lastBlock = size / blockSize;
		if(size % blockSize == 0) {
			lastBlock--;
		}
		size--;
		return blockList.get(lastBlock).remove(0);
	}

	public int size() {
		return size;
	}

	public void clear() {
		blockList.clear();
	}

	public boolean isEmpty() {
		return size == 0;
	}
}
