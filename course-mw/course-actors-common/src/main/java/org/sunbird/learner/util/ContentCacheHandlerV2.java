package org.sunbird.learner.util;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ContentCacheHandlerV2 {
    private Map<String, CachedContent> contentMap = new ConcurrentHashMap<>();
    private static ContentCacheHandlerV2 instance;
    private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();
    private LoggerUtil logger = new LoggerUtil(ContentCacheHandlerV2.class);
    private static final long LOCAL_TTL_MILLIS = 30 * 60 * 1000; // 30 minutes

    public static ContentCacheHandlerV2 getInstance() {
        if (instance == null) {
            synchronized (ContentCacheHandlerV2.class) {
                if (instance == null) {
                    instance = new ContentCacheHandlerV2();
                }
            }
        }
        return instance;
    }

    public Map<String, Object> getContent(String id) throws Exception {
        CachedContent cached = contentMap.get(id);
        if (cached != null && !cached.isExpired(LOCAL_TTL_MILLIS)) {
            return cached.content;
        }

        logger.info(null, "ContentCacheHandlerV2:getContent: Reading content from Redis for id: " + id);
        int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL));
        String cacheResponse = redisCacheUtil.getUsingIndex(id, null, ttl, 0);
        ObjectMapper mapper = new ObjectMapper();
        if (cacheResponse != null && !cacheResponse.trim().isEmpty() && !cacheResponse.trim().equals("{}")) {
            Map<String, Object> content = mapper.readValue(cacheResponse, new TypeReference<Map<String, Object>>() {
            });
            contentMap.put(id, new CachedContent(content));
            return content;
        } else {
            logger.info(null, "ContentCacheHandlerV2:getContent: Content not found in Redis for id: " + id);
            Map<String, Object> content = ContentUtil.getContentReadV3(id, null, null);
            if (content != null && !content.isEmpty()) {
                contentMap.put(id, new CachedContent(content));
                return content;
            }
        }
        return null;
    }

    public Map<String, Object> getExternalContent(String id) {
        CachedContent cached = contentMap.get(id);
        if (cached != null && !cached.isExpired(LOCAL_TTL_MILLIS)) {
            return cached.content;
        }
        Map<String, Object> fetched = ContentUtil.getAllExternalContent(
                Arrays.asList(id),
                Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH))
        );

        if (fetched != null && !fetched.isEmpty()) {
            contentMap.put(id, new CachedContent((Map<String, Object>) fetched.get(id)));
            return (Map<String, Object>) fetched.get(id);
        }
        return null;
    }

    /**
     * New sub class to hold the data and TTL value.
     */
    private static class CachedContent {
        Map<String, Object> content;
        long cachedTimeMillis;

        CachedContent(Map<String, Object> content) {
            this.content = content;
            this.cachedTimeMillis = System.currentTimeMillis();
        }

        boolean isExpired(long ttlMillis) {
            return System.currentTimeMillis() - cachedTimeMillis > ttlMillis;
        }
    }
}
