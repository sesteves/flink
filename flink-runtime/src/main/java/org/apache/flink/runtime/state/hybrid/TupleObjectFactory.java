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

import flexjson.JsonNumber;
import flexjson.ObjectBinder;
import flexjson.ObjectFactory;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Generalize this class to handle all kinds of tuples
 */
public class TupleObjectFactory implements ObjectFactory {

	@Override
	public Object instantiate(ObjectBinder context, Object value, Type targetType, Class targetClass) {
		if(value instanceof HashMap) {
			Map bindings = (Map)value;

			if(bindings.size() == 3) {
				Object o1 = bindings.get("_1");
				Object o2 = bindings.get("_2");
				if(o1 instanceof JsonNumber) {
					o1 = ((JsonNumber) o1).toInteger();
				}
				if(o2 instanceof JsonNumber) {
					o2 = ((JsonNumber) o2).toInteger();
				}
				return new Tuple2(o1, o2);
			}
			if(bindings.size() == 4) {
				Object o1 = bindings.get("_1");
				Object o2 = bindings.get("_2");
				Object o3 = bindings.get("_3");
				if(o1 instanceof JsonNumber) {
					o1 = ((JsonNumber) o1).toInteger();
				}
				if(o2 instanceof JsonNumber) {
					o2 = ((JsonNumber) o2).toInteger();
				}
				if(o3 instanceof JsonNumber) {
					o3 = ((JsonNumber) o3).toInteger();
				}
				return new Tuple3(o1, o2, o3);
			}
			if(bindings.size() == 5) {
				Object o1 = bindings.get("_1");
				Object o2 = bindings.get("_2");
				Object o3 = bindings.get("_3");
				Object o4 = bindings.get("_4");
				if(o1 instanceof JsonNumber) {
					o1 = ((JsonNumber) o1).toInteger();
				}
				if(o2 instanceof JsonNumber) {
					o2 = ((JsonNumber) o2).toInteger();
				}
				if(o3 instanceof JsonNumber) {
					o3 = ((JsonNumber) o3).toInteger();
				}
//				if(o4 instanceof JsonNumber) {
//					o4 = ((JsonNumber) o4).toInteger();
//				}

				return new Tuple4(o1, o2, o3, o4);
			}
		}
		System.out.println("### RETURNING NULL");
		return null;
	}
}
