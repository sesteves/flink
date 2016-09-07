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
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class BucketList<V> implements Iterator, Iterable {

	private List<V> primaryBucket;

	private int primaryBucketSize;

	private int primaryBucketIndex = 0;

	private BufferedReader br;

	private String line;

	private String firstLine;

	private boolean first = true;

	private PrintWriter secondaryBucket;

	JSONSerializer serializer = new JSONSerializer();

	private JSONDeserializer deserializer = new JSONDeserializer().use(Tuple2.class, new TupleObjectFactory());

	private long startTick = 0, endTick = 0;

	private PrintWriter stats;

	private boolean usePrimaryBucket = true;

	public BucketList(int primaryBucketSize) {
		primaryBucket = new ArrayList<>(primaryBucketSize);
		this.primaryBucketSize = primaryBucketSize;

		try {
			// autoflush set to true
			secondaryBucket = new PrintWriter(new FileWriter("state.txt"), true);

			br = new BufferedReader(new FileReader("state.txt"));
			stats = new PrintWriter(new FileOutputStream(new File("stats.txt"), true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		if(primaryBucketIndex < primaryBucket.size()  || line != null) {
			return true;
		} else {
			primaryBucketIndex = 0;
			endTick = System.currentTimeMillis();
			stats.println(endTick - startTick);
			stats.close();
			startTick = 0;
			endTick = 0;


			try {
				br.close();
				br = new BufferedReader(new FileReader("state.txt"));
			} catch (IOException e) {
				e.printStackTrace();
			}
			line = firstLine;
			return false;
		}
	}

	@Override
	public V next() {
		V result = null;
		if(primaryBucketIndex < primaryBucket.size()) {
			if(startTick == 0) {
				startTick = System.currentTimeMillis();
			}
			result = primaryBucket.get(primaryBucketIndex++);
		} else if(line != null) {
			if(endTick == 0) {
				endTick = System.currentTimeMillis();
				stats.print(endTick - startTick + ",");
				startTick = System.currentTimeMillis();
			}

			result = (V)deserializer.deserialize(line);
			try {
				line = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return result;
	}

	public void add(V value) {
		if(usePrimaryBucket && primaryBucket.size() <= primaryBucketSize) {
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
	}

	public void purge() {
		usePrimaryBucket = false;
		System.out.println("Primary Bucket Size: " + primaryBucket.size());
		for(int i = 0; i < primaryBucket.size(); i++) {
			V value = primaryBucket.remove(i);
			String json = serializer.serialize(value);
			if(first) {
				firstLine = json;
				line = firstLine;
				first = false;
			} else {
				secondaryBucket.println(json);
			}
		}
	}

	@Override
	public void remove() {
	}

	@Override
	public Iterator iterator() {
		return this;
	}
}
