package edu.snu.org.naive.util;

import edu.snu.org.naive.ReduceFunc;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Sliding Window for general reduce operation
 */
public class SlidingWindow <K, V> {

  private int bucketNum;
  ReduceFunc<V> reduceFunc;
  List<Bucket> bucketList;
  Bucket currentBucket;

  private class Bucket {
    private ConcurrentSkipListMap<K, V> innerMap;
    Bucket () {
      innerMap = new ConcurrentSkipListMap<>();
    }
    public V get(K key) {
      return innerMap.get(key);
    }
    public void reduce(K key, V val) {
      V oldVal = innerMap.get(key);
      boolean succ = false;
      do {
        if (oldVal == null) {
          succ = (null == (oldVal = innerMap.putIfAbsent(key, val)));
          if (succ) {
            break;
          }
        } else {
          V newVal = reduceFunc.compute(oldVal, val);
          succ = innerMap.replace(key, oldVal, newVal);
          if (!succ)
            oldVal = innerMap.get(key);
          else {
            break;
          }
        }
      } while (true);
    }
    public Set<K> keySet() {
      return innerMap.keySet();
    }
  }

  public SlidingWindow(int bucketNum, ReduceFunc<V> reduceFunc) {
    this.bucketNum = bucketNum;
    this.reduceFunc = reduceFunc;
    bucketList = new LinkedList<>();
    currentBucket = new Bucket();
    bucketList.add(currentBucket);
  }

  public void reduce(K key, V value) {
    currentBucket.reduce(key, value);
  }

  public void slideBucket() {
    bucketList.add(0, new Bucket());
    currentBucket = bucketList.get(0);
    if (bucketList.size() > bucketNum) {
      bucketList.remove(bucketList.size() - 1);
    }
  }

  public HashMap<K, V> getResultAndSlideBucket() {
    bucketList.add(0, new Bucket());
    currentBucket = bucketList.get(0);

    HashMap<K, V> result = new HashMap<>();
    for(int i = 1; i < bucketList.size(); i++) {
      Bucket bucket = bucketList.get(i);
      for(K key: bucket.keySet()) {
        if (result.containsKey(key)) {
          result.put(key, reduceFunc.compute(result.get(key), bucket.get(key)));
        }
        else {
          result.put(key, bucket.get(key));
        }
      }
    }
    if (bucketList.size() > bucketNum) {
      bucketList.remove(bucketList.size() - 1);
    }
    return result;
  }
}
