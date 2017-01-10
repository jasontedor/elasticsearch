package org.elasticsearch.benchmark.common.cache;

import org.elasticsearch.common.cache.Cache2Builder;
import org.elasticsearch.common.cache.CacheBuilder;

public enum CacheFactory {

    Cache {
        @Override
        <K, V> Adapter<K, V> create(int maximumSize) {
            return new CacheAdapter<>(CacheBuilder.<K, V>builder().setMaximumWeight(maximumSize).build());
        }
    },

    Cache2 {
        @Override
        <K, V> Adapter<K, V> create(int maximumSize) {
            return new Cache2Adapter<>(Cache2Builder.<K, V>builder().setMaximumWeight(maximumSize).build());
        }
    };

    abstract <K, V> Adapter<K, V> create(int maximumSize);
}
