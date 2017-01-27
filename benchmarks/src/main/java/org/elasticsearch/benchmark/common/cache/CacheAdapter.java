package org.elasticsearch.benchmark.common.cache;

import org.elasticsearch.common.cache.Cache;

import java.util.concurrent.ExecutionException;

public class CacheAdapter<K, V> implements Adapter<K, V> {

    private final Cache<K, V> cache;

    public CacheAdapter(final Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public V get(final K key) {
        return cache.get(key);
    }

    @Override
    public V put(final K key, V value) {
        try {
            return cache.computeIfAbsent(key, k -> value);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public void refresh() {
        cache.refresh();
    }
}
