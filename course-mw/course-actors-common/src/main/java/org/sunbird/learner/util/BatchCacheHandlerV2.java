package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.helper.ServiceFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchCacheHandlerV2 {

    private static BatchCacheHandlerV2 instance;


    private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();
    private LoggerUtil logger = new LoggerUtil(BatchCacheHandlerV2.class);
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Cache<String, Map<String, Object>> batchCache;

    private BatchCacheHandlerV2() {
        long ttlMinutes = Long.parseLong(PropertiesCache.getInstance().getProperty("BATCH_CACHE_TTL_MINUTES"));
        long maxSize = Long.parseLong(PropertiesCache.getInstance().getProperty("BATCH_CACHE_MAX_SIZE"));

       batchCache  = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .recordStats()
                .build();
    }

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
        Map<String, Object> content = batchCache.getIfPresent(batchId);
        if (content != null) {
            return content;
        }

        logger.info(null, "BatchCacheHandlerV2:getContent: Reading content from Redis for id: " + batchId);

        int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL));
        String cacheResponse = redisCacheUtil.getUsingIndex(batchId, null, ttl, 0);

        ObjectMapper mapper = new ObjectMapper();

        if (cacheResponse != null && !cacheResponse.trim().isEmpty() && !cacheResponse.trim().equals("{}")) {
            content = mapper.readValue(cacheResponse, new TypeReference<Map<String, Object>>() {});
            batchCache.put(batchId, content);
            return content;
        } else {
            logger.info(null, "BatchCacheHandlerV2:getContent: Content not found in Redis for id: " + batchId);

            Map<String, Object> primaryKey = new HashMap<>();
            primaryKey.put(JsonKey.COURSE_ID, courseId);
            primaryKey.put(JsonKey.BATCH_ID, batchId);

            Response response = cassandraOperation.getRecordByIdentifier(
                    null,
                    "sunbird_courses",
                    "course_batch",
                    primaryKey,
                    null
            );

            if (response != null && response.getResult() != null) {
                Object resultObj = response.getResult().get("response");

                if (resultObj instanceof List) {
                    List<?> responseList = (List<?>) resultObj;
                    if (!responseList.isEmpty() && responseList.get(0) instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fetchedContent = (Map<String, Object>) responseList.get(0);
                        if (fetchedContent != null && !fetchedContent.isEmpty()) {
                            batchCache.put(batchId, fetchedContent);
                            redisCacheUtil.set(batchId, mapper.writeValueAsString(fetchedContent), Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL)));
                            return fetchedContent;
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
}