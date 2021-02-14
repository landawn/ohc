/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
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
package org.caffinitas.ohc.benchmark.distribution;

import java.util.Random;

import org.apache.commons.math3.random.RandomGenerator;

// based on http://en.wikipedia.org/wiki/Xorshift, but periodically we reseed with our stronger random generator
// note it is also non-atomically updated, so expects to be used by a single thread
public class FasterRandom implements RandomGenerator {
    final Random random = new Random();

    private long seed;
    private int reseed;

    @Override
    public void setSeed(int seed) {
        setSeed((long) seed);
    }

    @Override
    public void setSeed(int[] ints) {
        if (ints.length > 1) {
            setSeed(((long) ints[0] << 32) | ints[1]);
        } else {
            setSeed(ints[0]);
        }
    }

    @Override
    public void setSeed(long seed) {
        this.seed = seed;
        rollover();
    }

    private void rollover() {
        this.reseed = 0;
        random.setSeed(seed);
        seed = random.nextLong();
    }

    @Override
    public void nextBytes(byte[] bytes) {
        int i = 0;
        while (i < bytes.length) {
            long next = nextLong();
            while (i < bytes.length) {
                bytes[i++] = (byte) (next & 0xFF);
                next >>>= 8;
            }
        }
    }

    @Override
    public int nextInt() {
        return (int) nextLong();
    }

    @Override
    public int nextInt(int i) {
        return Math.abs((int) nextLong() % i);
    }

    @Override
    public long nextLong() {
        if (++this.reseed == 32) {
            rollover();
        }

        long seed = this.seed;
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        this.seed = seed;
        return seed * 2685821657736338717L;
    }

    @Override
    public boolean nextBoolean() {
        return ((int) nextLong() & 1) == 1;
    }

    @Override
    public float nextFloat() {
        return Float.intBitsToFloat((int) nextLong());
    }

    @Override
    public double nextDouble() {
        return Double.longBitsToDouble(nextLong());
    }

    @Override
    public double nextGaussian() {
        return random.nextGaussian();
    }
}
