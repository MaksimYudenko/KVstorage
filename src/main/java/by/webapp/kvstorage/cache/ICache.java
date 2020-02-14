package by.webapp.kvstorage.cache;

public interface ICache<K, V> {

    V get(K key);

    V put(K key, V value);

    V putIfAbsent(K key, V value);

    V remove(K key);

    boolean contains(K key);

    long size();

}