package edu.snu.org.naive;

import edu.snu.org.util.ReduceFunc;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Sliding Window for general reduce operation
 */
public class SlidingWindow <K, V> implements Serializable {

  private int bucketNum;
  ReduceFunc<V> reduceFunc;
  List<Bucket> bucketList;
  Bucket currentBucket;

  private class Bucket implements Serializable {
    private Map<K, V> innerMap;
    Bucket () {
      innerMap = new HashMap<>();
    }
    public V get(K key) {
      return innerMap.get(key);
    }
    public void reduce(K key, V val) {
      if (innerMap.containsKey(key)) {
        V sofar = innerMap.get(key);
        innerMap.put(key,reduceFunc.compute(val, sofar));
      }
      else {
        innerMap.put(key, val);
      }
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

  public Map<K, V> getResultAndSlideBucket() {
    bucketList.add(0, new Bucket());
    currentBucket = bucketList.get(0);

    Map<K, V> result = new HashMap<>();
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
