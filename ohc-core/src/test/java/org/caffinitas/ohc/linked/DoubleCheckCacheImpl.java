/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.caffinitas.ohc.linked;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.caffinitas.ohc.CacheLoader;
import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.DirectValueAccess;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.testng.Assert;

/**
 * Test code that contains an instance of the production and check {@link org.caffinitas.ohc.OHCache}
 * implementations {@link OHCacheLinkedImpl} and
 * {@link CheckOHCacheImpl}.
 */
public class DoubleCheckCacheImpl<K, V> implements OHCache<K, V> {
    public final OHCache<K, V> prod;
    public final OHCache<K, V> check;

    public DoubleCheckCacheImpl(OHCacheBuilder<K, V> builder) {
        this.prod = builder.build();
        this.check = new CheckOHCacheImpl<>(builder);
    }

    @Override
    public boolean put(K key, V value) {
        boolean rProd = prod.put(key, value);
        boolean rCheck = check.put(key, value);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public boolean addOrReplace(K key, V old, V value) {
        boolean rProd = prod.addOrReplace(key, old, value);
        boolean rCheck = check.addOrReplace(key, old, value);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public boolean putIfAbsent(K k, V v) {
        boolean rProd = prod.putIfAbsent(k, v);
        boolean rCheck = check.putIfAbsent(k, v);
        Assert.assertEquals(rProd, rCheck, "for key='" + k + '\'');
        return rProd;
    }

    @Override
    public boolean putIfAbsent(K key, V value, long expireAt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addOrReplace(K key, V old, V value, long expireAt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean put(K key, V value, long expireAt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        prod.putAll(m);
        check.putAll(m);
    }

    @Override
    public boolean remove(K key) {
        boolean rProd = prod.remove(key);
        boolean rCheck = check.remove(key);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public void removeAll(Iterable<K> keys) {
        prod.removeAll(keys);
        check.removeAll(keys);
    }

    @Override
    public void clear() {
        prod.clear();
        check.clear();
    }

    @Override
    public DirectValueAccess getDirect(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectValueAccess getDirect(K key, boolean updateLRU) {
        throw new UnsupportedOperationException();
    }

    public DirectValueAccess putDirect(K key, long valueLen) {
        throw new UnsupportedOperationException();
    }

    public DirectValueAccess addOrReplaceDirect(K k, DirectValueAccess old, long valueLen) {
        throw new UnsupportedOperationException();
    }

    public DirectValueAccess putIfAbsentDirect(K k, long valueLen) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(K key) {
        V rProd = prod.get(key);
        V rCheck = check.get(key);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public boolean containsKey(K key) {
        boolean rProd = prod.containsKey(key);
        boolean rCheck = check.containsKey(key);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public V getWithLoader(K key, CacheLoader<K, V> loader) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getWithLoader(K key, CacheLoader<K, V> loader, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<V> getWithLoaderAsync(K key, CacheLoader<K, V> loader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<V> getWithLoaderAsync(K key, CacheLoader<K, V> loader, long expireAt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloseableIterator<K> hotKeyIterator(int n) {
        return new CheckIterator<>(prod.hotKeyIterator(n), check.hotKeyIterator(n), true);
    }

    @Override
    public CloseableIterator<K> keyIterator() {
        return new CheckIterator<>(prod.keyIterator(), check.keyIterator(), true);
    }

    @Override
    public CloseableIterator<ByteBuffer> hotKeyBufferIterator(int n) {
        return new CheckIterator<>(prod.hotKeyBufferIterator(n), check.hotKeyBufferIterator(n), false);
    }

    @Override
    public CloseableIterator<ByteBuffer> keyBufferIterator() {
        return new CheckIterator<>(prod.keyBufferIterator(), check.keyBufferIterator(), false);
    }

    @Override
    public boolean deserializeEntry(ReadableByteChannel channel) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean serializeEntry(K key, WritableByteChannel channel) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int deserializeEntries(ReadableByteChannel channel) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int serializeHotNEntries(int n, WritableByteChannel channel) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int serializeHotNKeys(int n, WritableByteChannel channel) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloseableIterator<K> deserializeKeys(ReadableByteChannel channel) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetStatistics() {
        prod.resetStatistics();
        check.resetStatistics();
    }

    @Override
    public long size() {
        long rProd = prod.size();
        long rCheck = check.size();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public int[] hashTableSizes() {
        return prod.hashTableSizes();
    }

    @Override
    public long[] perSegmentSizes() {
        long[] rProd = prod.perSegmentSizes();
        long[] rCheck = check.perSegmentSizes();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public EstimatedHistogram getBucketHistogram() {
        return prod.getBucketHistogram();
    }

    @Override
    public int segments() {
        int rProd = prod.segments();
        int rCheck = check.segments();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public long capacity() {
        long rProd = prod.capacity();
        long rCheck = check.capacity();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public long memUsed() {
        long rProd = prod.memUsed();
        long rCheck = check.memUsed();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public long freeCapacity() {
        long rProd = prod.freeCapacity();
        long rCheck = check.freeCapacity();
        Assert.assertEquals(rProd, rCheck, "capacity: " + capacity());
        return rProd;
    }

    @Override
    public float loadFactor() {
        float rProd = prod.loadFactor();
        float rCheck = check.loadFactor();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public OHCacheStats stats() {
        OHCacheStats rProd = prod.stats();
        OHCacheStats rCheck = check.stats();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public void setCapacity(long capacity) {
        prod.setCapacity(capacity);
        check.setCapacity(capacity);
    }

    @Override
    public void close() throws IOException {
        prod.close();
        check.close();
    }

    private class CheckIterator<T> implements CloseableIterator<T> {
        private final CloseableIterator<T> prodIter;
        private final CloseableIterator<T> checkIter;

        private final boolean canCompare;

        private final Set<T> prodReturned = new HashSet<>();
        private final Set<T> checkReturned = new HashSet<>();

        CheckIterator(CloseableIterator<T> prodIter, CloseableIterator<T> checkIter, boolean canCompare) {
            this.prodIter = prodIter;
            this.checkIter = checkIter;
            this.canCompare = canCompare;
        }

        @Override
        public void close() throws IOException {
            prodIter.close();
            checkIter.close();

            Assert.assertEquals(prodReturned.size(), checkReturned.size());
            if (canCompare) {
                for (T t : prodReturned) {
                    Assert.assertTrue(check.containsKey((K) t), "check does not contain key " + t);
                }
                for (T t : checkReturned) {
                    Assert.assertTrue(prod.containsKey((K) t), "prod does not contain key " + t);
                }
            }
        }

        @Override
        public boolean hasNext() {
            boolean rProd = prodIter.hasNext();
            boolean rCheck = checkIter.hasNext();
            Assert.assertEquals(rProd, rCheck);
            return rProd;
        }

        @Override
        public T next() {
            T rProd = prodIter.next();
            T rCheck = checkIter.next();
            prodReturned.add(rProd);
            checkReturned.add(rCheck);
            return rProd;
        }

        @Override
        public void remove() {
            prodIter.remove();
            checkIter.remove();
        }
    }
}
