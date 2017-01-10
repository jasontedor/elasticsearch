/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.cache;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.ToLongBiFunction;

public class Cache2<K, V> {

    // positive if entries have an expiration
    private long expireAfterAccessNanos = -1;

    // true if entries can expire after access
    private boolean entriesExpireAfterAccess;

    void setExpireAfterAccessNanos(final long expireAfterAccessNanos) {
        if (expireAfterAccessNanos <= 0) {
            throw new IllegalArgumentException("expireAfterAccessNanos <= 0");
        }
        this.expireAfterAccessNanos = expireAfterAccessNanos;
        this.entriesExpireAfterAccess = true;
    }

    // positive if entries have an expiration after write
    private long expireAfterWriteNanos = -1;

    // true if entries can expire after initial insertion
    private boolean entriesExpireAfterWrite;

    void setExpireAfterWriteNanos(final long expireAfterWriteNanos) {
        if (expireAfterWriteNanos <= 0) {
            throw new IllegalArgumentException("expireAfterWriteNanos <= 0");
        }
        this.expireAfterWriteNanos = expireAfterWriteNanos;
        this.entriesExpireAfterWrite = true;
    }

    // the number of entries in the cache
    private LongAdder count = new LongAdder();

    public long count() {
        return count.longValue();
    }

    // the weight of the entries in the cache
    private LongAdder weight = new LongAdder();

    public long weight() {
        return weight.longValue();
    }

    // the maximum weight that this cache supports
    private long maximumWeight = -1;

    void setMaximumWeight(final long maximumWeight) {
        if (maximumWeight < 0) {
            throw new IllegalArgumentException("maximumWeight < 0");
        }
        this.maximumWeight = maximumWeight;
    }

    // the weigher of entries
    private ToLongBiFunction<K, V> weigher = (k, v) -> 1;

    void setWeigher(final ToLongBiFunction<K, V> weigher) {
        Objects.requireNonNull(weigher);
        this.weigher = weigher;
    }

    private RemovalListener<K, V> removalListener = notification -> {
    };

    void setRemovalListener(final RemovalListener<K, V> removalListener) {
        Objects.requireNonNull(removalListener);
        this.removalListener = removalListener;
    }

    protected long now() {
        /*
         * System.nanoTime takes non-negligible time, so we only use it if we need it; we use System#nanoTime because we want relative time,
         * not absolute time.
         */
        return entriesExpireAfterAccess || entriesExpireAfterWrite ? System.nanoTime() : 0;
    }

    private boolean isExpired(final CacheEntry<K, V> cacheEntry, final long now) {
        return (entriesExpireAfterAccess && now - cacheEntry.accessTime > expireAfterAccessNanos) ||
            (entriesExpireAfterWrite && now - cacheEntry.writeTime > expireAfterWriteNanos);
    }

    public V get(final K key) {
        return get(key, now());
    }

    private V get(final K key, final long now) {
        final CacheSegment<K, V> segment = getCacheSegment(key);
        final CacheEntry<K, V> cacheEntry = segment.get(key, now);
        if (cacheEntry == null || isExpired(cacheEntry, now)) {
            return null;
        } else {
            promote(cacheEntry, now);
            return cacheEntry.value;
        }
    }

    public void put(final K key, final V value) {
        put(key, value, now());
    }

    private void put(final K key, final V value, final long now) {
        final CacheSegment<K, V> segment = getCacheSegment(key);
        Tuple<CacheEntry<K, V>, CacheEntry<K, V>> tuple = segment.put(key, value, now);
        boolean replaced = tuple.v2() != null && deletedUpdater.compareAndSet(tuple.v2().node, Boolean.FALSE, Boolean.TRUE);
        if (replaced) {
            removalListener.onRemoval(
                new RemovalNotification<>(tuple.v2().key, tuple.v2().value, RemovalNotification.RemovalReason.REPLACED));
            weight.add(-weigher.applyAsLong(tuple.v2().key, tuple.v2().value));
        } else {
            count.increment();
        }
        weight.add(weigher.applyAsLong(key, value));
        promote(tuple.v1(), now);
    }

    public V computeIfAbsent(final K key, final CacheLoader<K, V> loader) throws ExecutionException {
        final long now = now();
        V value = get(key, now);
        if (value == null) {
            // we need to synchronize loading of a value for a given key; however, holding the segment lock while
            // invoking load can lead to deadlock against another thread due to dependent key loading; therefore, we
            // need a mechanism to ensure that load is invoked at most once, but we are not invoking load while holding
            // the segment lock; to do this, we atomically put a future in the map that can load the value, and then
            // get the value from this future on the thread that won the race to place the future into the segment map
            CacheSegment<K, V> segment = getCacheSegment(key);
            CompletableFuture<CacheEntry<K, V>> future;
            CompletableFuture<CacheEntry<K, V>> completableFuture = new CompletableFuture<>();

            try (ReleasableLock ignored = segment.writeLock.acquire()) {
                future = segment.map.putIfAbsent(key, completableFuture);
            }

            BiFunction<? super CacheEntry<K, V>, Throwable, ? extends V> handler = (ok, ex) -> {
                if (ok != null) {
                    promote(ok, now);
                    return ok.value;
                } else {
                    try (ReleasableLock ignored = segment.writeLock.acquire()) {
                        CompletableFuture<CacheEntry<K, V>> sanity = segment.map.get(key);
                        if (sanity != null && sanity.isCompletedExceptionally()) {
                            segment.map.remove(key);
                        }
                    }
                    return null;
                }
            };

            CompletableFuture<V> completableValue;
            if (future == null) {
                future = completableFuture;
                completableValue = future.handle(handler);
                V loaded;
                try {
                    loaded = loader.load(key);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                    throw new ExecutionException(e);
                }
                if (loaded == null) {
                    NullPointerException npe = new NullPointerException("loader returned a null value");
                    future.completeExceptionally(npe);
                    throw new ExecutionException(npe);
                } else {
                    future.complete(new CacheEntry<>(key, loaded, now));
                    count.increment();
                    weight.add(weigher.applyAsLong(key, loaded));
                }
            } else {
                completableValue = future.handle(handler);
            }

            try {
                value = completableValue.get();
                // check to ensure the future hasn't been completed with an exception
                if (future.isCompletedExceptionally()) {
                    future.get(); // call get to force the exception to be thrown for other concurrent callers
                    throw new IllegalStateException("the future was completed exceptionally but no exception was thrown");
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
        return value;
    }

    private void promote(final CacheEntry<K, V> cacheEntry, final long now) {
        offer(cacheEntry);
        evict(now);
    }

    public void invalidate(final K key) {
        final CacheSegment<K, V> segment = getCacheSegment(key);
        final CacheEntry<K, V> cacheEntry = segment.remove(key);
        if (cacheEntry != null) {
            if (deletedUpdater.compareAndSet(cacheEntry.node, Boolean.FALSE, Boolean.TRUE)) {
                removalListener.onRemoval(
                    new RemovalNotification<>(cacheEntry.key, cacheEntry.value, RemovalNotification.RemovalReason.INVALIDATED));
                count.decrement();
                weight.add(-weigher.applyAsLong(cacheEntry.key, cacheEntry.value));
            }
        }
    }

    public void invalidateAll() {
        final boolean[] haveSegmentLock = new boolean[NUMBER_OF_SEGMENTS];
        try {
            for (int i = 0; i < haveSegmentLock.length; i++) {
                segments[i].segmentLock.writeLock().lock();
                haveSegmentLock[i] = true;
            }
            Arrays.stream(segments).forEach(segment -> segment.map = new HashMap<>());
            while (!eviction.compareAndSet(false, true)) {
                Thread.yield();
            }
            LinkedListNode<K, V> current = head;
            while (current != null) {
                if (deletedUpdater.compareAndSet(current, Boolean.FALSE, Boolean.TRUE)) {
                    if (current.cacheEntry != null) {
                        removalListener.onRemoval(
                            new RemovalNotification<>(
                                current.cacheEntry.key,
                                current.cacheEntry.value,
                                RemovalNotification.RemovalReason.INVALIDATED));
                    }
                }
                current = current.after;
            }
            head = tail = new LinkedListNode<>();
            count = new LongAdder();
            weight = new LongAdder();
            eviction.set(false);
        } finally {
            for (int i = haveSegmentLock.length - 1; i >= 0; i--) {
                if (haveSegmentLock[i]) {
                    segments[i].segmentLock.writeLock().unlock();
                }
            }
        }
    }

    public void refresh() {
        evict(now());
    }

    private void evict(final long now) {
        if (eviction.compareAndSet(false, true)) {
            while (head != null && shouldPrune(head, now)) {
                LinkedListNode<K, V> pruned = poll();
                if (pruned == null) return;
                final CacheEntry<K, V> cacheEntry = pruned.cacheEntry;
                if (cacheEntry != null) {
                    final CacheSegment<K, V> segment = getCacheSegment(cacheEntry.key);
                    if (segment != null) {
                        segment.remove(cacheEntry.key);
                    }
                    removalListener.onRemoval(
                        new RemovalNotification<>(cacheEntry.key, cacheEntry.value, RemovalNotification.RemovalReason.EVICTED));
                    if (deletedUpdater.compareAndSet(pruned, Boolean.FALSE, Boolean.TRUE)) {
                        count.decrement();
                        weight.add(-weigher.applyAsLong(cacheEntry.key, cacheEntry.value));
                    }
                }
            }
            eviction.set(false);
        }
    }

    private boolean shouldPrune(final LinkedListNode<K, V> entry, long now) {
        return exceedsWeight() || entry.cacheEntry == null || isExpired(entry.cacheEntry, now) || entry.deleted;
    }

    private boolean exceedsWeight() {
        return maximumWeight != -1 && weight.longValue() > maximumWeight;
    }

    public Iterable<K> keys() {
        return () -> new Iterator<K>() {

            private final CacheIterator iterator = new CacheIterator(head);

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public K next() {
                return iterator.next().cacheEntry.key;
            }

        };
    }

    private class CacheIterator implements Iterator<LinkedListNode<K, V>> {

        private LinkedListNode<K, V> current;
        private LinkedListNode<K, V> next;

        CacheIterator(LinkedListNode<K, V> head) {
            current = null;
            next = head;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public LinkedListNode<K, V> next() {
            current = next;
            next = next.after;
            while (next != null && (next.cacheEntry == null || next.deleted)) {
                next = next.after;
            }
            return current;
        }

    }

    static class CacheEntry<K, V> {

        final K key;
        final V value;
        final long writeTime;
        volatile long accessTime;
        volatile LinkedListNode<K, V> node;

        CacheEntry(final K key, final V value, final long writeTime) {
            this.key = key;
            this.value = value;
            this.writeTime = this.accessTime = writeTime;
        }

    }

    private static final int NUMBER_OF_SEGMENTS = 256;
    @SuppressWarnings("unchecked")
    private final Cache2.CacheSegment<K, V>[] segments = new Cache2.CacheSegment[NUMBER_OF_SEGMENTS];

    {
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new CacheSegment<>();
        }
    }

    private CacheSegment<K, V> getCacheSegment(final K key) {
        return segments[key.hashCode() & 0xff];
    }

    static class CacheSegment<K, V> {

        final ReadWriteLock segmentLock = new ReentrantReadWriteLock();

        final ReleasableLock readLock = new ReleasableLock(segmentLock.readLock());
        final ReleasableLock writeLock = new ReleasableLock(segmentLock.writeLock());

        Map<K, CompletableFuture<CacheEntry<K, V>>> map = new HashMap<>();

        final SegmentStats segmentStats = new SegmentStats();

        public CacheEntry<K, V> get(final K key, final long now) {
            final CompletableFuture<CacheEntry<K, V>> future;
            CacheEntry<K, V> cacheEntry = null;
            try (final ReleasableLock ignored = readLock.acquire()) {
                future = map.get(key);
            }
            if (future != null) {
                try {
                    cacheEntry = future.handle((ok, ex) -> {
                        if (ok != null) {
                            segmentStats.hit();
                            ok.accessTime = now;
                            return ok;
                        } else {
                            segmentStats.miss();
                            return null;
                        }
                    }).get();
                } catch (final ExecutionException | InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                segmentStats.miss();
            }
            return cacheEntry;
        }

        Tuple<CacheEntry<K, V>, CacheEntry<K, V>> put(final K key, final V value, final long now) {
            final CacheEntry<K, V> cacheEntry = new CacheEntry<>(key, value, now);
            CacheEntry<K, V> existing = null;
            try (final ReleasableLock ignored = writeLock.acquire()) {
                try {
                    CompletableFuture<CacheEntry<K, V>> future = map.put(key, CompletableFuture.completedFuture(cacheEntry));
                    if (future != null) {
                        existing = future.handle((ok, ex) -> {
                            if (ok != null) {
                                return ok;
                            } else {
                                return null;
                            }
                        }).get();
                    }
                } catch (final ExecutionException | InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            return Tuple.tuple(cacheEntry, existing);
        }

        CacheEntry<K, V> remove(final K key) {
            CompletableFuture<CacheEntry<K, V>> future;
            CacheEntry<K, V> entry = null;
            try (ReleasableLock ignored = writeLock.acquire()) {
                future = map.remove(key);
            }
            if (future != null) {
                try {
                    entry = future.handle((ok, ex) -> {
                        if (ok != null) {
                            segmentStats.eviction();
                            return ok;
                        } else {
                            return null;
                        }
                    }).get();
                } catch (final ExecutionException | InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            return entry;
        }

        private static class SegmentStats {

            private final LongAdder hits = new LongAdder();
            private final LongAdder misses = new LongAdder();
            private final LongAdder evictions = new LongAdder();

            void hit() {
                hits.increment();
            }

            void miss() {
                misses.increment();
            }

            void eviction() {
                evictions.increment();
            }
        }

    }

    public CacheStats stats() {
        long hits = 0;
        long misses = 0;
        long evictions = 0;
        for (int i = 0; i < segments.length; i++) {
            hits += segments[i].segmentStats.hits.longValue();
            misses += segments[i].segmentStats.misses.longValue();
            evictions += segments[i].segmentStats.evictions.longValue();
        }
        return new CacheStats(hits, misses, evictions);
    }

    public static class CacheStats {

        private final long hits;

        public long hits() {
            return hits;
        }

        private final long misses;

        public long misses() {
            return misses;
        }

        private final long evictions;

        public long evictions() {
            return evictions;
        }

        public CacheStats(final long hits, final long misses, final long evictions) {
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
        }

    }

    volatile LinkedListNode<K, V> head;
    volatile LinkedListNode<K, V> tail;

    {
        final LinkedListNode<K, V> node = new LinkedListNode<>();
        head = tail = node;
    }

    final AtomicBoolean eviction = new AtomicBoolean();

    static class LinkedListNode<K, V> {

        CacheEntry<K, V> cacheEntry;
        volatile LinkedListNode<K, V> after;
        volatile Boolean deleted = Boolean.FALSE;

    }

    private void offer(final CacheEntry<K, V> cacheEntry) {
        if (tail.cacheEntry != cacheEntry) {
            final LinkedListNode<K, V> n = cacheEntry.node;
            final LinkedListNode<K, V> node = new LinkedListNode<>();
            node.cacheEntry = cacheEntry;
            if (cacheEntryNodeUpdater.compareAndSet(cacheEntry, n, node)) {
                while (true) {
                    final LinkedListNode<K, V> t = tail;
                    final LinkedListNode<K, V> ta = t.after;
                    if (t == tail) {
                        if (ta != null) {
                            tailUpdater.compareAndSet(this, t, ta);
                        } else {
                            if (afterUpdater.compareAndSet(t, null, node)) {
                                tailUpdater.compareAndSet(this, t, node);
                            }
                            break;
                        }
                    }
                }
                if (n != null) {
                    n.cacheEntry = null;
                }
            }
        }
    }

    private LinkedListNode<K, V> poll() {
        while (true) {
            final LinkedListNode<K, V> h = head;
            final LinkedListNode<K, V> t = tail;
            final LinkedListNode<K, V> ha = head.after;
            if (h == head) {
                if (h == t) {
                    if (ha == null) {
                        return null;
                    }
                    tailUpdater.compareAndSet(this, t, ha);
                } else {
                    if (headUpdater.compareAndSet(this, h, ha)) {
                        return h;
                    }
                }
            }
        }
    }

    AtomicReferenceFieldUpdater<CacheEntry, LinkedListNode> cacheEntryNodeUpdater =
        AtomicReferenceFieldUpdater.newUpdater(CacheEntry.class, LinkedListNode.class, "node");

    AtomicReferenceFieldUpdater<Cache2, LinkedListNode> headUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Cache2.class, LinkedListNode.class, "head");

    AtomicReferenceFieldUpdater<Cache2, LinkedListNode> tailUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Cache2.class, LinkedListNode.class, "tail");

    AtomicReferenceFieldUpdater<LinkedListNode, LinkedListNode> afterUpdater =
        AtomicReferenceFieldUpdater.newUpdater(LinkedListNode.class, LinkedListNode.class, "after");

    AtomicReferenceFieldUpdater<LinkedListNode, Boolean> deletedUpdater =
        AtomicReferenceFieldUpdater.newUpdater(LinkedListNode.class, Boolean.class, "deleted");

    @FunctionalInterface
    public interface CacheLoader<K, V> {

        V load(K key) throws Exception;

    }

    public static class RemovalNotification<K, V> {

        public enum RemovalReason {REPLACED, INVALIDATED, EVICTED}

        private final K key;
        private final V value;
        private final RemovalNotification.RemovalReason removalReason;

        public RemovalNotification(final K key, final V value, final RemovalNotification.RemovalReason removalReason) {
            this.key = key;
            this.value = value;
            this.removalReason = removalReason;
        }

        public K key() {
            return key;
        }

        public V value() {
            return value;
        }

        public RemovalNotification.RemovalReason removalReason() {
            return removalReason;
        }

    }

    @FunctionalInterface
    public interface RemovalListener<K, V> {

        void onRemoval(RemovalNotification<K, V> notification);

    }

}
