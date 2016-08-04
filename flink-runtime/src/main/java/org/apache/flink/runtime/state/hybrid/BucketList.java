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
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class BucketList<V> implements Iterator, Iterable {

	private static final int PRIMARY_BUCKET_SIZE = 100000;

	private List<V> primaryBucket = new ArrayList<>(PRIMARY_BUCKET_SIZE);

	private BufferedReader br;

	private String line;

	private PrintWriter secondaryBucket;

	JSONSerializer serializer = new JSONSerializer();

	private JSONDeserializer deserializer = new JSONDeserializer().use(Tuple2.class, new TupleObjectFactory());

	public BucketList() {
		try {
			secondaryBucket = new PrintWriter("state.txt");

			br = new BufferedReader(new FileReader("state.txt"));
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		return primaryBucket.size() > 0 || line != null;
	}

	@Override
	public V next() {
		V result = null;
		if(primaryBucket.size() > 0) {
			result = primaryBucket.remove(0);
		} else if(line != null) {
			result = (V)deserializer.deserialize(line);
			try {
				line = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	// TODO thread to load primaryBucket from secondaryBucket
//	private void secondaryToPrimaryBucket() {
//	}

	public void add(V value) {
		if(primaryBucket.size() <= PRIMARY_BUCKET_SIZE) {
			primaryBucket.add(value);
		} else {
			String json = serializer.serialize(value);
			secondaryBucket.println(json);
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
