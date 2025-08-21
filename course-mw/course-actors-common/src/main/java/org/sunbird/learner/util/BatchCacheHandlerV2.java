package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.helper.ServiceFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BatchCacheHandlerV2 {
    private Map<String, BatchCacheHandlerV2.CachedContent> contentMap = new ConcurrentHashMap<>();
    private static BatchCacheHandlerV2 instance;
    private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();
    private LoggerUtil logger = new LoggerUtil(BatchCacheHandlerV2.class);
    private static final long LOCAL_TTL_MILLIS = 30 * 60 * 1000; // 30 minutes
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

    public static BatchCacheHandlerV2 getInstance() {
        if (instance == null) {
            synchronized (BatchCacheHandlerV2.class) {
                if (instance == null) {
                    instance = new BatchCacheHandlerV2();
                }
            }
        }
        return instance;
    }

    public Map<String, Object> getContent(String batchId, String courseId) throws Exception {
        BatchCacheHandlerV2.CachedContent cached = contentMap.get(batchId);
        if (cached != null && !cached.isExpired(LOCAL_TTL_MILLIS)) {
            return cached.content;
        }

        logger.info(null, "BatchCacheHandlerV2:getContent: Reading content from Redis for id: " + batchId);
        int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL));
        String cacheResponse = redisCacheUtil.getUsingIndex(batchId, null, ttl, 0);
        ObjectMapper mapper = new ObjectMapper();
        if (cacheResponse != null && !cacheResponse.trim().isEmpty() && !cacheResponse.trim().equals("{}")) {
            Map<String, Object> content = mapper.readValue(cacheResponse, new TypeReference<Map<String, Object>>() {
            });
            contentMap.put(batchId, new BatchCacheHandlerV2.CachedContent(content));
            return content;
        } else {
            logger.info(null, "BatchCacheHandlerV2:getContent: Content not found in Redis for id: " + batchId);
            Map<String, Object> primaryKey = new HashMap<>();
            primaryKey.put(JsonKey.COURSE_ID, courseId);
            primaryKey.put(JsonKey.BATCH_ID, batchId);
            Response response = cassandraOperation.getRecordByIdentifier(null, "sunbird_courses", "course_batch", primaryKey, null);
            if (response != null && response.getResult() != null) {
                Object resultObj = response.getResult().get("response");
                if (resultObj instanceof List) {
                    List<?> responseList = (List<?>) resultObj;
                    if (!responseList.isEmpty() && responseList.get(0) instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> content = (Map<String, Object>) responseList.get(0);
                        if (content != null && !content.isEmpty()) {
                            contentMap.put(batchId, new BatchCacheHandlerV2.CachedContent(content));
                            return content;
                        } else {
                            logger.info(null, "BatchCacheHandlerV2:getContent: Empty content for batchId: " + batchId);
                        }
                    } else {
                        logger.info(null, "BatchCacheHandlerV2:getContent: Unexpected response format for batchId: " + batchId);
                    }
                } else {
                    logger.info(null, "BatchCacheHandlerV2:getContent: Response object is not a list for batchId: " + batchId);
                }
            } else {
                logger.info(null, "BatchCacheHandlerV2:getContent: Null response or result from Cassandra for batchId: " + batchId);
            }
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
