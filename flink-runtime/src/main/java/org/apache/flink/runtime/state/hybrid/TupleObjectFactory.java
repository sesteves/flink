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

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TupleObjectFactory implements ObjectFactory {

	@Override
	public Object instantiate(ObjectBinder context, Object value, Type targetType, Class targetClass) {
		if(value instanceof HashMap) {
			Map bindings = (Map)value;
			return new Tuple2(bindings.get("_1"), ((JsonNumber)bindings.get("_2")).toInteger());
		}
		System.out.println("### RETURNING NULL");
		return null;
	}
}
