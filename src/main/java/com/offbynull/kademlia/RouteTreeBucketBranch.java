/*
 * Copyright (c) 2017, Kasra Faghihi, All rights reserved.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */
package com.offbynull.kademlia;

import org.apache.commons.lang3.Validate;

class RouteTreeBucketBranch implements RouteTreeBranch {

    private final KBucket kBucket;

    RouteTreeBucketBranch(KBucket kBucket) {
        Validate.notNull(kBucket);
        this.kBucket = kBucket;
    }

    @Override
    public BitString getPrefix() {
        return kBucket.getPrefix();
    }

    @Override
    @SuppressWarnings("unchecked")
    public KBucket getItem() {
        return kBucket;
    }
}
