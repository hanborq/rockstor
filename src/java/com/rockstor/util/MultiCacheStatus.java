/**
 * Copyright 2012 Hanborq Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rockstor.util;

import java.util.concurrent.atomic.AtomicLong;

public class MultiCacheStatus implements MultiCacheStatusMBean {
    private MultiCache cache;
    private AtomicLong hit_num;
    private AtomicLong lost_num;
    private AtomicLong overload_num;
    private AtomicLong total_req;
    private AtomicLong drop_num;
    private AtomicLong remove_num;

    private long hitLast = 0;
    private long lostLast = 0;
    private long overloadLast = 0;
    private long dropLast = 0;
    private long removeLast = 0;

    private long hitTotalLast = 0;
    private long lostTotalLast = 0;
    private long overloadTotalLast = 0;
    private long dropTotalLast = 0;
    private long removeTotalLast = 0;

    public MultiCacheStatus(MultiCache cache) {
        this.cache = cache;
        hit_num = new AtomicLong(0);
        lost_num = new AtomicLong(0);
        total_req = new AtomicLong(0); // total ask num
        drop_num = new AtomicLong(0); // lru out num
        remove_num = new AtomicLong(0); // read file exception
        overload_num = new AtomicLong(0);
    }

    @Override
    public long getDropRate() {
        long curTotal = total_req.get();
        long dropCur = drop_num.get();

        long ret = 0;

        if (curTotal != dropTotalLast) {
            ret = ((dropCur - dropLast) * 100) / (curTotal - dropTotalLast);

            dropTotalLast = curTotal;
            dropLast = dropCur;
        }

        return ret;
    }

    @Override
    public long getFailedRate() {
        long curTotal = total_req.get();
        long removeCur = remove_num.get();

        long ret = 0;

        if (curTotal != removeTotalLast) {
            ret = ((removeCur - removeLast) * 100)
                    / (curTotal - removeTotalLast);

            removeTotalLast = curTotal;
            removeLast = removeCur;
        }

        return ret;
    }

    @Override
    public long getHitRate() {
        long curTotal = total_req.get();
        long hitCur = hit_num.get();

        long ret = 0;

        if (curTotal != hitTotalLast) {
            ret = ((hitCur - hitLast) * 100) / (curTotal - hitTotalLast);

            hitTotalLast = curTotal;
            hitLast = hitCur;
        }

        return ret;
    }

    @Override
    public long getLostRate() {
        long curTotal = total_req.get();
        long lostCur = lost_num.get();

        long ret = 0;

        if (curTotal != lostTotalLast) {
            ret = ((lostCur - lostLast) * 100) / (curTotal - lostTotalLast);

            lostTotalLast = curTotal;
            lostLast = lostCur;
        }

        return ret;
    }

    @Override
    public long getOverloadRate() {
        long curTotal = total_req.get();
        long overloadCur = overload_num.get();

        long ret = 0;

        if (curTotal != overloadTotalLast) {
            ret = ((overloadCur - overloadLast) * 100)
                    / (curTotal - overloadTotalLast);

            overloadTotalLast = curTotal;
            overloadLast = overloadCur;
        }

        return ret;
    }

    public long request() {
        return total_req.incrementAndGet();
    }

    public long hit() {
        return hit_num.incrementAndGet();
    }

    public long lost() {
        return lost_num.incrementAndGet();
    }

    public long overload() {
        return overload_num.incrementAndGet();
    }

    public long drop() {
        return drop_num.incrementAndGet();
    }

    public long remove() {
        return remove_num.incrementAndGet();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[Cache Status: ");
        sb.append("hit=");
        sb.append(hit_num.get());
        sb.append(", lost=");
        sb.append(lost_num.get());
        sb.append(", total_req=");
        sb.append(total_req.get());
        sb.append(", drop_num=");
        sb.append(drop_num.get());
        sb.append(", remove_num=");
        sb.append(remove_num.get());
        sb.append(", overload_num=");
        sb.append(overload_num.get());
        sb.append("]");

        return sb.toString();
    }

    @Override
    public int getBusyNum() {
        return cache.getBusyNum();
    }

    @Override
    public int getMaxNum() {
        return cache.getMaxNum();
    }

    @Override
    public int getSize() {
        return cache.getSize();
    }

}
