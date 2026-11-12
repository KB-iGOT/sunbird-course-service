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

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchCacheHandlerV2 {

    private static BatchCacheHandlerV2 instance;

    private LoggerUtil logger = new LoggerUtil(BatchCacheHandlerV2.class);
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Cache<String, Map<String, Object>> batchCache;
    private ObjectMapper mapper = new ObjectMapper();

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
            logger.info(null, "BatchCacheHandlerV2:getContent: local cache (Caffeine) hit for batchId: " + batchId);
            return content;
        }

        logger.info(null, "BatchCacheHandlerV2:getContent: Cache miss. Fetching from Cassandra (DB) for id: " + batchId);

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
                        if (fetchedContent.containsKey(JsonKey.START_DATE) && fetchedContent.get(JsonKey.START_DATE) instanceof Date) {
                            Date startDate = (Date) fetchedContent.get(JsonKey.START_DATE);
                            String formattedStartDate = new SimpleDateFormat("yyyy-MM-dd").format(startDate);
                            fetchedContent.put(JsonKey.START_DATE, formattedStartDate);
                        }
                        if (fetchedContent.containsKey(JsonKey.UPDATED_DATE) && fetchedContent.get(JsonKey.UPDATED_DATE) instanceof Date) {
                            Date updatedDate = (Date) fetchedContent.get(JsonKey.UPDATED_DATE);
                            String formattedUpdatedDate = new SimpleDateFormat("yyyy-MM-dd").format(updatedDate);
                            fetchedContent.put(JsonKey.UPDATED_DATE, formattedUpdatedDate);
                        }
                        Object batchAttrObj = fetchedContent.get(JsonKey.BATCH_ATTRIBUTES);
                        if (batchAttrObj != null && batchAttrObj instanceof String) {
                            try {
                                Map<String, Object> attrMap = mapper.readValue((String) batchAttrObj, new TypeReference<Map<String, Object>>() {});
                                fetchedContent.put(JsonKey.BATCH_ATTRIBUTES, attrMap);
                            } catch (Exception e) {
                                logger.error(null,"Failed to parse batch_attributes JSON for batchId: " + batchId, e);
                            }
                        }
                        batchCache.put(batchId, fetchedContent);
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

        return null;
    }
}