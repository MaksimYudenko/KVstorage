package by.webapp.kvstorage.cache;

import java.util.HashMap;
import java.util.Map;

public class LRUCache<K, V> implements ICache<K, V> {

    private final Map<K, CacheNode<K, V>> cacheMap;
    private CacheNode<K, V> head, tail;
    private final int capacity;

    public LRUCache() {
        this(10);
    }

    public LRUCache(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException(
                "Wrong input data: 'capacity' is illegal");
        this.cacheMap = new HashMap<>();
        this.capacity = capacity;
    }

    @Override
    public synchronized V get(K key) {
        if (cacheMap.containsKey(key)) {
            CacheNode<K, V> cacheNode = cacheMap.get(key);
            removeNode(cacheNode);
            setHead(cacheNode);
            return cacheNode.v;
        }
        return null;
    }

    @Override
    public synchronized V put(K key, V value) {
        return contains(key) ?
                doPut(key, value, false) : putIfAbsent(key, value);
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        return doPut(key, value, true);
    }

    @Override
    public synchronized V remove(K key) {
        CacheNode<K, V> currentNode = cacheMap.remove(key);
        return currentNode == null ? null : currentNode.v;
    }

    @Override
    public synchronized boolean contains(K key) {
        return cacheMap.containsKey(key);
    }

    @Override
    public synchronized long size() {
        return cacheMap.size();
    }

    private V doPut(K key, V value, boolean isAbsent) {
        V oldValue = null;
        CacheNode<K, V> currentNode = cacheMap.get(key);
        if (isAbsent) {
            if (cacheMap.containsKey(key)) throw new IllegalArgumentException("The item is present. Use put()");
            currentNode = new CacheNode<>(key, value, null, null);
            if (cacheMap.size() == capacity) {
                cacheMap.remove(tail.k);
                removeNode(tail);
                setHead(currentNode);
            } else setHead(currentNode);
            cacheMap.put(key, currentNode);
        } else {
            oldValue = currentNode.v;
            currentNode.v = value;
            removeNode(currentNode);
            setHead(currentNode);
        }
        return oldValue;
    }

    private void setHead(CacheNode<K, V> node) {
        node.next = head;
        node.previous = null;
        if (head != null) head.previous = node;
        head = node;
        if (tail == null) tail = head;
    }

    private void removeNode(CacheNode<K, V> node) {
        if (node.previous != null) node.previous.next = node.next;
        else head = node.next;
        if (node.next != null) node.next.previous = node.previous;
        else tail = node.previous;
    }

    private static class CacheNode<Key, Value> {

        private Key k;
        private Value v;
        private CacheNode<Key, Value> previous;
        private CacheNode<Key, Value> next;

        CacheNode(Key k, Value v, CacheNode<Key, Value> previous,
                  CacheNode<Key, Value> next) {
            this.k = k;
            this.v = v;
            this.previous = previous;
            this.next = next;
        }

    }

}