package by.webapp.kvstorage.cache;

import by.webapp.kvstorage.exception.BadRequestException;

public class CacheFactory<T> {

    public ICache<String, T> create(String algorithmName, int cacheLimit) {
        if (!algorithmName.equalsIgnoreCase("LFU") &&
                !algorithmName.equalsIgnoreCase("LRU")) {
            throw new BadRequestException(
                    "Error: algorithm might be either LFU or LRU.");
        } else {
            Cache cache = Cache.valueOf(algorithmName.toUpperCase());
            return cache.getCache(cacheLimit);
        }
    }

    public enum Cache {

        LFU {
            @Override
            public ICache getCache(int size) {
                return new LFUCache(size);
            }
        },
        LRU {
            @Override
            public ICache getCache(int size) {
                return new LRUCache(size);
            }
        };

        public abstract ICache getCache(int size);

    }

}