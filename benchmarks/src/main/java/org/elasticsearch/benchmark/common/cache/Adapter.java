package org.elasticsearch.benchmark.common.cache;

interface Adapter<K, V> {

    V get(K key);

    V put(K key, V value);

    void clear();

    void refresh();

}
