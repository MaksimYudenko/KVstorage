package by.webapp.kvstorage.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

public class LFUCache<K, V> implements ICache<K, V> {

    private final Map<K, CacheNode<K, V>> cacheMap;
    private final LinkedHashSet<CacheNode<K, V>>[] frequencyList;
    private final int capacity;
    private final float evictionFactor;
    private int minFrequency;
    private int maxFrequency;

    public LFUCache() {
        this(10);
    }

    public LFUCache(int capacity) {
        this(capacity, 0.75f);
    }

    public LFUCache(int capacity, float evictionFactor) {
        if (capacity <= 0 || evictionFactor <= 0 || evictionFactor >= 1) {
            throw new IllegalArgumentException(
                    "Wrong input data: 'evictionFactor' out of range 0...1 or " +
                            "'capacity' is illegal");
        }
        this.cacheMap = new HashMap<>(capacity);
        this.frequencyList = new LinkedHashSet[capacity];
        this.capacity = capacity;
        this.evictionFactor = evictionFactor;
        this.minFrequency = 0;
        this.maxFrequency = capacity - 1;
        for (int i = 0; i <= maxFrequency; i++) {
            frequencyList[i] = new LinkedHashSet<>();
        }
    }

    @Override
    public synchronized V get(K key) {
        CacheNode<K, V> currentNode = cacheMap.get(key);
        if (currentNode == null) {
            return null;
        } else {
            int currentFrequency = currentNode.frequency;
            LinkedHashSet<CacheNode<K, V>> currentNodes = frequencyList[currentFrequency];
            if (currentFrequency < maxFrequency) {
                int nextFrequency = currentFrequency + 1;
                LinkedHashSet<CacheNode<K, V>> newNodes = frequencyList[nextFrequency];
                moveToNextFrequency(currentNode, nextFrequency, currentNodes, newNodes);
                cacheMap.put(key, currentNode);
                if (minFrequency == currentFrequency && currentNodes.isEmpty()) {
                    minFrequency = nextFrequency;
                }
            } else {
                // add most recently used item ahead of others:
                currentNodes.remove(currentNode);
                currentNodes.add(currentNode);
            }
            return currentNode.v;
        }
    }

    @Override
    public synchronized V put(K key, V value) {
        V oldValue = null;
        CacheNode<K, V> currentNode = cacheMap.get(key);
        if (currentNode == null) {
            if (cacheMap.size() == capacity) {
                doEviction();
            }
            LinkedHashSet<CacheNode<K, V>> nodes = frequencyList[0];
            currentNode = new CacheNode<>(key, value, 0);
            nodes.add(currentNode);
            cacheMap.put(key, currentNode);
            minFrequency = 0;
        } else {
            oldValue = currentNode.v;
            currentNode.v = value;
        }
        return oldValue;
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        return contains(key) ? value : put(key, value);
    }

    @Override
    public synchronized V remove(K key) {
        CacheNode<K, V> currentNode = cacheMap.remove(key);
        if (currentNode != null) {
            LinkedHashSet<CacheNode<K, V>> nodes = frequencyList[currentNode.frequency];
            nodes.remove(currentNode);
            if (minFrequency == currentNode.frequency) {
                findNextLowestFrequency();
            }
            return currentNode.v;
        } else {
            return null;
        }
    }

    @Override
    public synchronized boolean contains(K key) {
        return cacheMap.containsKey(key);
    }

    @Override
    public synchronized long size() {
        return cacheMap.size();
    }

    private void doEviction() {
        int currentlyDeleted = 0;
        float target = capacity * evictionFactor;
        while (currentlyDeleted < target) {
            LinkedHashSet<CacheNode<K, V>> nodes = frequencyList[minFrequency];
            if (nodes.isEmpty()) {
                throw new IllegalStateException("Lowest frequency constraint violated.");
            } else {
                Iterator<CacheNode<K, V>> it = nodes.iterator();
                while (it.hasNext() && currentlyDeleted++ < target) {
                    CacheNode<K, V> node = it.next();
                    it.remove();
                    cacheMap.remove(node.k);
                }
                if (!it.hasNext()) {
                    findNextLowestFrequency();
                }
            }
        }
    }

    private void moveToNextFrequency(
            CacheNode<K, V> currentNode, int nextFrequency,
            LinkedHashSet<CacheNode<K, V>> currentNodes,
            LinkedHashSet<CacheNode<K, V>> newNodes) {
        currentNodes.remove(currentNode);
        newNodes.add(currentNode);
        currentNode.frequency = nextFrequency;
    }

    private void findNextLowestFrequency() {
        while (minFrequency <= maxFrequency && frequencyList[minFrequency].isEmpty()) {
            minFrequency++;
        }
        if (minFrequency > maxFrequency) {
            minFrequency = 0;
        }
    }

    private static class CacheNode<Key, Value> {

        private final Key k;
        private Value v;
        private int frequency;

        CacheNode(Key k, Value v, int frequency) {
            this.k = k;
            this.v = v;
            this.frequency = frequency;
        }

    }

}