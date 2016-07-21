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
import scala.Tuple2;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

/**
 *
 */
public class BucketList<V> implements Iterator, Iterable {

//	private List bucket1 = new ArrayList();
//	private List bucket2 = new ArrayList();

	// private BufferedReader br;
	// private FileReader reader;
	private RandomAccessFile raf;

	private String line;

	private JSONDeserializer deserializer = new JSONDeserializer().use(Tuple2.class, new TupleObjectFactory());

	public BucketList() {
		try {
			//br = new BufferedReader(new FileReader("state.txt"));
			// line = br.readLine();
			// reader = new FileReader("state.txt");
			raf = new RandomAccessFile("state.txt", "r");
			line = raf.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

//	private String readLine() {
//		StringBuilder sb = new StringBuilder();
//		try {
//			for (int chr = reader.read(); chr != '\n' && chr != -1; chr = reader.read()) {
//				sb.append((char) chr);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return sb.toString();
//	}




	@Override
	public boolean hasNext() {
		return line != null;
	}

	@Override
	public V next() {
		V result = (V)deserializer.deserialize(line);
		try {
			line = raf.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public void remove() {
	}

	@Override
	public Iterator iterator() {
		return this;
	}
}
