package org.caffinitas.ohc.benchmark.distribution;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

public class DistributionInverted extends Distribution {

    private static final long serialVersionUID = 2610490464931791839L;
    final Distribution wrapped;
    final long min;
    final long max;

    public DistributionInverted(Distribution wrapped) {
        this.wrapped = wrapped;
        this.min = wrapped.minValue();
        this.max = wrapped.maxValue();
    }

    @Override
    public long next() {
        return max - (wrapped.next() - min);
    }

    @Override
    public double nextDouble() {
        return max - (wrapped.nextDouble() - min);
    }

    @Override
    public long inverseCumProb(double cumProb) {
        return max - (wrapped.inverseCumProb(cumProb) - min);
    }

    @Override
    public void setSeed(long seed) {
        wrapped.setSeed(seed);
    }

    public static Distribution invert(Distribution distribution) {
        if (distribution instanceof DistributionInverted) {
            return ((DistributionInverted) distribution).wrapped;
        }
        return new DistributionInverted(distribution);
    }

}
