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
    private Map<String, Object> contentMap = new ConcurrentHashMap<>();
    private static ContentCacheHandlerV2 instance;
    private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();
    private LoggerUtil logger = new LoggerUtil(ContentCacheHandlerV2.class);

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
        if (contentMap.containsKey(id)) {
            return (Map<String, Object>) contentMap.get(id);
        } else {
            logger.info(null, "ContentCacheHandlerV2:getContent: Reading content from Redis for id: " + id);
            int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL));
            String cacheResponse = redisCacheUtil.getUsingIndex(id, null, ttl, 0);
            ObjectMapper mapper = new ObjectMapper();
            if (cacheResponse != null && !cacheResponse.trim().isEmpty() && !cacheResponse.trim().equals("{}")) {
                contentMap.put(id, mapper.readValue(cacheResponse, new TypeReference<Map<String, Object>>() {
                }));
                return (Map<String, Object>) contentMap.get(id);
            } else {
                logger.info(null, "ContentCacheHandlerV2:getContent: Content not found in Redis for id: " + id);
                Map<String, Object> content = ContentUtil.getContentReadV3(id, null, null);
                if (content != null && !content.isEmpty()) {
                    contentMap.put(id, content);
                    return content;
                }
            }
        }
        return null;
    }

    public Map<String, Object> getExternalContent(String id) {
        Map<String, Object> obj = (Map<String, Object>) contentMap.get(id);
        if (obj != null)
            return obj;
        else {
            contentMap
                    .putAll(ContentUtil.getAllExternalContent(Arrays.asList(id),
                            Integer.parseInt(PropertiesCache.getInstance()
                                    .getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH))));
            return (Map<String, Object>) contentMap.get(id);
        }
    }
}
