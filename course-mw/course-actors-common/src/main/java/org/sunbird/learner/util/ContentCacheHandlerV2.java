package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;

import java.time.Duration;
import java.util.Map;

public class ContentCacheHandlerV2 {
    private static ContentCacheHandlerV2 instance;
    private LoggerUtil logger = new LoggerUtil(ContentCacheHandlerV2.class);
    private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();

    private final Cache<String, Map<String, Object>> contentCache;
    private ContentCacheHandlerV2() {
        long ttlMinutes = Long.parseLong(PropertiesCache.getInstance().getProperty("CONTENT_CACHE_TTL_MINUTES"));
        long maxSize = Long.parseLong(PropertiesCache.getInstance().getProperty("CONTENT_CACHE_MAX_SIZE"));

        contentCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .recordStats()
                .build();
    }


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
        Map<String, Object> content = contentCache.getIfPresent(id);
        if (content != null) return content;

        logger.info(null, "ContentCacheHandlerV2:getContent: Reading content from Redis for id: " + id);
        int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL));
        String cacheResponse = redisCacheUtil.getUsingIndex(id, null, ttl, 0);
        ObjectMapper mapper = new ObjectMapper();
        if (cacheResponse != null && !cacheResponse.trim().isEmpty() && !cacheResponse.trim().equals("{}")) {
            content = mapper.readValue(cacheResponse, new TypeReference<Map<String, Object>>() {});
            contentCache.put(id, content);
            return content;
        }
        logger.info(null, "ContentCacheHandlerV2:getContent: Content not found in Redis for id: " + id);
        content = ContentUtil.getContentReadV3(id, null, null);

        if (content != null && !content.isEmpty()) {
            contentCache.put(id, content);
            redisCacheUtil.set(id, mapper.writeValueAsString(content), ttl);
            return content;
        }

        return null;
    }

    public Map<String, Object> getExternalContent(String id) throws Exception {
        Map<String, Object> content = contentCache.getIfPresent(id);
        if (content != null) return content;

        Map<String, Object> fetched = ContentUtil.getAllExternalContent(
                java.util.Arrays.asList(id),
                Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH))
        );

        if (fetched != null && !fetched.isEmpty()) {
            Map<String, Object> data = (Map<String, Object>) fetched.get(id);
            contentCache.put(id, data);
            return data;
        }
        return null;
    }
}