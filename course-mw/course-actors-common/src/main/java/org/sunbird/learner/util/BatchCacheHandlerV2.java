package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
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
    private LoadingCache<String, Map<String, Object>> batchCache;
    private ObjectMapper mapper = new ObjectMapper();

    private BatchCacheHandlerV2() {
        long ttlMinutes = Long.parseLong(PropertiesCache.getInstance().getProperty("BATCH_CACHE_TTL_MINUTES"));
        long maxSize = Long.parseLong(PropertiesCache.getInstance().getProperty("BATCH_CACHE_MAX_SIZE"));

       // refreshAfterWrite from Redis/Cassandra (courseId taken from the cached row). reload: error -> old; gone -> removed.
       batchCache  = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(Duration.ofMinutes(ttlMinutes))
                .recordStats()
                .build(new CacheLoader<String, Map<String, Object>>() {
                    @Override public Map<String, Object> load(String batchId) { return null; } // courseId unknown: callers use getIfPresent/put
                    @Override public Map<String, Object> reload(String batchId, Map<String, Object> old) {
                        String courseId = old == null ? null : (String) old.get(JsonKey.COURSE_ID);
                        if (courseId == null) return old;
                        try {
                            Map<String, Object> row = CourseBatchCacheUtil.get(courseId, batchId);
                            if (row == null) row = readFromCassandra(batchId, courseId);
                            return row == null ? null : decorate(new HashMap<>(row), batchId);
                        } catch (Exception e) {
                            return old;
                        }
                    }
                });
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

        // Redis first (shared cache written on batch create/update), then Cassandra
        Map<String, Object> row = CourseBatchCacheUtil.get(courseId, batchId);
        if (row != null) {
            logger.info(null, "BatchCacheHandlerV2:getContent: cache hit (Redis) for batchId: " + batchId);
        } else {
            logger.info(null, "BatchCacheHandlerV2:getContent: Cache miss. Fetching from Cassandra (DB) for id: " + batchId);
            row = readFromCassandra(batchId, courseId);
            if (row == null) return null;
            CourseBatchCacheUtil.put(row);
        }

        Map<String, Object> decorated = decorate(new HashMap<>(row), batchId);
        batchCache.put(batchId, decorated);
        return decorated;
    }

    /** Drop this pod's in-memory copy; called by CourseBatchCacheUtil.evict on batch update. */
    public void invalidate(String batchId) {
        if (batchId != null) batchCache.invalidate(batchId);
    }

    private Map<String, Object> readFromCassandra(String batchId, String courseId) {
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
        if (response == null || response.getResult() == null) {
            logger.info(null, "BatchCacheHandlerV2:getContent: Null response or result from Cassandra for batchId: " + batchId);
            return null;
        }
        Object resultObj = response.getResult().get("response");
        if (!(resultObj instanceof List)) {
            logger.info(null, "BatchCacheHandlerV2:getContent: Response object is not a list for batchId: " + batchId);
            return null;
        }
        List<?> responseList = (List<?>) resultObj;
        if (responseList.isEmpty() || !(responseList.get(0) instanceof Map)) {
            logger.info(null, "BatchCacheHandlerV2:getContent: Unexpected response format for batchId: " + batchId);
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) responseList.get(0);
        if (row.isEmpty()) {
            logger.info(null, "BatchCacheHandlerV2:getContent: Empty content for batchId: " + batchId);
            return null;
        }
        row.remove(JsonKey.PARTICIPANT);
        return row;
    }

    /** Response-shape adjustments applied to a copy of the raw row (dates as yyyy-MM-dd, batch_attributes parsed). */
    private Map<String, Object> decorate(Map<String, Object> fetchedContent, String batchId) {
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
        return fetchedContent;
    }
}