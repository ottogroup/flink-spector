/*
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.flinkspector.core.quantify;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * Wraps a {@link Tuple} with keys to it's values
 *
 * @param <T>
 */
public class TupleMap<T extends Tuple> {

    private final T tuple;
    private String[] keys;

    /**
     * Default constructor
     *
     * @param tuple wrapped {@link Tuple}.
     * @param keys  key strings.
     */
    public TupleMap(T tuple, String[] keys) {
        if (keys.length > tuple.getArity()) {
            throw new IllegalArgumentException("Number of keys is greater" +
                    " than the arity of the tuple!");
        }
        this.tuple = tuple;
        this.keys = keys;
    }

    /**
     * Returns the value of a key.
     *
     * @param key  string.
     * @param <IN> return type.
     * @return value
     */
    public <IN> IN get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key has to be not null!");
        }
        int index = ArrayUtils.indexOf(keys, key);
        if (index < 0) {
            throw new IllegalArgumentException("Key \"" + key + "\" not found!");
        }
        return tuple.getField(index);
    }

    /**
     * Provides the list of keys
     *
     * @return Array of string keys.
     */
    public String[] getKeys() {
        return keys;
    }

    public String toString() {
        return tuple.toString();
    }

}
