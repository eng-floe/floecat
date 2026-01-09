/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.shaded.org.apache.commons.collections4.map;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Minimal stand-in for Hadoop's relocated commons-collections class.
 *
 * <p>Only the static {@code unmodifiableMap} factory is required by Hadoop configuration classes,
 * so this implementation simply wraps the provided map in an unmodifiable delegate from {@link
 * java.util.Collections}.
 */
public final class UnmodifiableMap<K, V> implements Map<K, V>, Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<K, V> delegate;

  private UnmodifiableMap(Map<? extends K, ? extends V> source) {
    Map<K, V> base = source == null ? Collections.emptyMap() : new LinkedHashMap<>(source);
    this.delegate = Collections.unmodifiableMap(base);
  }

  public static <K, V> Map<K, V> unmodifiableMap(Map<? extends K, ? extends V> map) {
    if (map instanceof UnmodifiableMap<?, ?> unmodifiable) {
      @SuppressWarnings("unchecked")
      Map<K, V> cast = (Map<K, V>) unmodifiable;
      return cast;
    }
    return new UnmodifiableMap<>(map);
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return delegate.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return delegate.get(key);
  }

  @Override
  public V put(K key, V value) {
    throw new UnsupportedOperationException("Map is unmodifiable");
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException("Map is unmodifiable");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException("Map is unmodifiable");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Map is unmodifiable");
  }

  @Override
  public Set<K> keySet() {
    return delegate.keySet();
  }

  @Override
  public Collection<V> values() {
    return delegate.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof UnmodifiableMap<?, ?> that) {
      return Objects.equals(this.delegate, that.delegate);
    }
    return delegate.equals(o);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }
}
