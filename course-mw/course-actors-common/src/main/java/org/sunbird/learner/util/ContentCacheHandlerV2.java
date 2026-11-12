package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import net.logstash.logback.encoder.org.apache.commons.lang3.StringUtils;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContentCacheHandlerV2 {
    private static ContentCacheHandlerV2 instance;
    private LoggerUtil logger = new LoggerUtil(ContentCacheHandlerV2.class);
    private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();

    private final Cache<String, Map<String, Object>> contentCache;
    /** Max keys per MGET against the shared Redis (content_cache_mget_batch_size, default 50). */
    private final int mgetBatchSize;
    /**
     * Negative cache: ids that neither Redis nor the content API returned. Without it a retired or unknown
     * course costs a Redis miss plus a synchronous HTTP call on every request that lists it.
     * TTL content_cache_negative_ttl_seconds (default 60) bounds how long a newly published course can be unseen.
     */
    private final Cache<String, Boolean> missingCache;
    private ContentCacheHandlerV2() {
        long ttlMinutes = Long.parseLong(PropertiesCache.getInstance().getProperty("CONTENT_CACHE_TTL_MINUTES"));
        long maxSize = Long.parseLong(PropertiesCache.getInstance().getProperty("CONTENT_CACHE_MAX_SIZE"));
        mgetBatchSize = Math.max(1, intProp(PropertiesCache.getInstance(), "content_cache_mget_batch_size", 50));
        long negativeTtl = Math.max(1, intProp(PropertiesCache.getInstance(), "content_cache_negative_ttl_seconds", 60));
        missingCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofSeconds(negativeTtl))
                .build();

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

    /**
     * Batched lookup for many course ids: Caffeine getAllPresent, then ONE Redis MGET for the misses,
     * then the content read API only for ids Redis lacks (which repopulates content-service's Redis).
     * Fills Caffeine as a side effect, so subsequent getContent(id) calls for these ids are hits.
     * Returns id -> content for ids that resolved; ids that resolve to nothing are absent.
     */
    public Map<String, Map<String, Object>> getContents(Collection<String> ids) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        if (ids == null || ids.isEmpty()) return result;
        Set<String> distinct = new LinkedHashSet<>();
        for (String id : ids) if (StringUtils.isNotBlank(id)) distinct.add(id);

        result.putAll(contentCache.getAllPresent(distinct));
        if (!result.isEmpty()) {
            logger.info(null, "ContentCacheHandlerV2:getContents: local cache (Caffeine) hit for " + result.size() + " of " + distinct.size() + " ids");
        }
        List<String> misses = new ArrayList<>();
        for (String id : distinct) if (!result.containsKey(id) && missingCache.getIfPresent(id) == null) misses.add(id);
        if (misses.isEmpty()) return result;

        // MGET in small chunks: content values are full objects (5-30 KB each) and this Redis is shared
        // with other services, so one command must stay small. Admin (_rc) and regular ids share index 0.
        Map<String, String> fromRedis = new HashMap<>();
        for (int from = 0; from < misses.size(); from += mgetBatchSize) {
            List<String> chunk = misses.subList(from, Math.min(from + mgetBatchSize, misses.size()));
            try {
                fromRedis.putAll(redisCacheUtil.mgetUsingIndex(chunk, 0));
            } catch (Exception e) {
                logger.error(null, "ContentCacheHandlerV2:getContents: Redis MGET failed for a chunk of " + chunk.size() + " ids", e);
            }
        }
        ObjectMapper mapper = new ObjectMapper();
        List<String> stillMissing = new ArrayList<>();
        int redisHits = 0;
        for (String id : misses) {
            String json = fromRedis.get(id);
            if (StringUtils.isNotBlank(json) && !StringUtils.equals(StringUtils.trim(json), "{}")) {
                try {
                    Map<String, Object> content = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                    contentCache.put(id, content);
                    result.put(id, content);
                    redisHits++;
                    continue;
                } catch (Exception e) {
                    logger.error(null, "ContentCacheHandlerV2:getContents: bad JSON in Redis for id: " + id, e);
                }
            }
            stillMissing.add(id);
        }
        if (redisHits > 0) {
            logger.info(null, "ContentCacheHandlerV2:getContents: cache hit (Redis) for " + redisHits + " of " + misses.size() + " ids");
        }
        if (!stillMissing.isEmpty()) {
            logger.info(null, "ContentCacheHandlerV2:getContents: " + stillMissing.size() + " of " + distinct.size() + " ids not in Redis, falling back to content API (DB)");
        }
        int apiHits = 0;
        for (String id : stillMissing) {
            Map<String, Object> content = id.endsWith("_rc")
                    ? ContentUtil.getAdminContentReadV4(id, null, null)
                    : ContentUtil.getContentReadV3(id, null, null);
            if (content != null && !content.isEmpty()) {
                contentCache.put(id, content);
                result.put(id, content);
                apiHits++;
            } else {
                missingCache.put(id, Boolean.TRUE);
            }
        }
        if (!stillMissing.isEmpty()) {
            logger.info(null, "ContentCacheHandlerV2:getContents: content API (DB) resolved " + apiHits + " of " + stillMissing.size() + " ids");
        }
        return result;
    }

    /**
     * Startup pre-warm: course ids listed in the shared-Redis key lms_top_course_ids_key (a SET, or a HASH
     * whose fields are course ids) are loaded into Caffeine via getContents, one MGET chunk at a time with a
     * pause between chunks so a pod start never bursts the shared instance. An absent or empty key means no
     * pre-warm. Never throws: a failure here must not block application start.
     */
    public void warmFromTopCourses() {
        PropertiesCache props = PropertiesCache.getInstance();
        String key = prop(props, "lms_top_course_ids_key", "lms-top-course-ids");
        int index = intProp(props, "lms_top_course_ids_redis_index", 0);
        long pauseMs = intProp(props, "content_cache_warmup_pause_ms", 50);
        long start = System.currentTimeMillis();
        try {
            List<String> ids = redisCacheUtil.getIdsUsingIndex(key, index);
            if (ids == null || ids.isEmpty()) {
                logger.info(null, "ContentCacheHandlerV2:warmFromTopCourses: " + key + " is empty, skipping pre-warm");
                return;
            }
            int loaded = 0;
            for (int from = 0; from < ids.size(); from += mgetBatchSize) {
                List<String> chunk = ids.subList(from, Math.min(from + mgetBatchSize, ids.size()));
                loaded += getContents(chunk).size();
                if (pauseMs > 0 && from + mgetBatchSize < ids.size()) Thread.sleep(pauseMs);
            }
            logger.info(null, "ContentCacheHandlerV2:warmFromTopCourses: loaded " + loaded + " of " + ids.size()
                    + " courses from " + key + " in " + (System.currentTimeMillis() - start) + " ms");
        } catch (Exception e) {
            logger.error(null, "ContentCacheHandlerV2:warmFromTopCourses: pre-warm aborted after "
                    + (System.currentTimeMillis() - start) + " ms", e);
        }
    }

    private static String prop(PropertiesCache props, String key, String def) {
        String v = props.getProperty(key);
        return (StringUtils.isBlank(v) || key.equals(v)) ? def : v.trim();
    }

    private static int intProp(PropertiesCache props, String key, int def) {
        try { return Integer.parseInt(prop(props, key, String.valueOf(def))); } catch (NumberFormatException e) { return def; }
    }

    public Map<String, Object> getContent(String id) throws Exception {
        Map<String, Object> content = contentCache.getIfPresent(id);
        if (content != null) {
            logger.info(null, "ContentCacheHandlerV2:getContent: local cache (Caffeine) hit for id: " + id);
            return content;
        }
        if (missingCache.getIfPresent(id) != null) return null;
        int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL));
        ObjectMapper mapper = new ObjectMapper();
        try {
            logger.info(null, "ContentCacheHandlerV2:getContent: Reading content from Redis for id: " + id);
            String cacheResponse = redisCacheUtil.getUsingIndex(id, null, ttl, 0);
            if (StringUtils.isNotBlank(cacheResponse) && !StringUtils.equals(StringUtils.trim(cacheResponse), "{}")) {
                content = mapper.readValue(cacheResponse, new TypeReference<Map<String, Object>>() {});
                contentCache.put(id, content);
                logger.info(null, "ContentCacheHandlerV2:getContent: cache hit (Redis) for id: " + id);
                return content;
            }
        } catch (Exception e) {
            logger.error(null, "ContentCacheHandlerV2:getContent: Error while reading content from Redis for id: " + id, e);
        }
        logger.info(null, "ContentCacheHandlerV2:getContent: Content not found in Redis for id: " + id + ", falling back to content API (DB)");
        content = ContentUtil.getContentReadV3(id, null, null);

        if (content != null && !content.isEmpty()) {
            contentCache.put(id, content);
            logger.info(null, "ContentCacheHandlerV2:getContent: fetched from content API (DB) for id: " + id);
            //redisCacheUtil.set(id, mapper.writeValueAsString(content), ttl);
            return content;
        }

        missingCache.put(id, Boolean.TRUE);
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

    public Map<String, Object> getAdminContent(String id) throws Exception {
        Map<String, Object> content = contentCache.getIfPresent(id);
        if (content != null) {
            logger.info(null, "ContentCacheHandlerV2:getAdminContent: local cache (Caffeine) hit for id: " + id);
            return content;
        }
        if (missingCache.getIfPresent(id) != null) return null;
        int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL));
        ObjectMapper mapper = new ObjectMapper();
        try {
            logger.info(null, "ContentCacheHandlerV2:getAdminContent: Reading content from Redis for id: " + id);
            String cacheResponse = redisCacheUtil.getUsingIndex(id, null, ttl, 0);
            if (StringUtils.isNotBlank(cacheResponse) && !StringUtils.equals(StringUtils.trim(cacheResponse), "{}")) {
                content = mapper.readValue(cacheResponse, new TypeReference<Map<String, Object>>() {
                });
                contentCache.put(id, content);
                logger.info(null, "ContentCacheHandlerV2:getAdminContent: cache hit (Redis) for id: " + id);
                return content;
            }
        } catch (Exception e) {
            logger.error(null, "ContentCacheHandlerV2:getAdminContent: Error while reading content from Redis for id: " + id, e);
        }
        logger.info(null, "ContentCacheHandlerV2:getAdminContent: Content not found in Redis for id: " + id + ", falling back to content API (DB)");
        content = ContentUtil.getAdminContentReadV4(id, null, null);

        if (content != null && !content.isEmpty()) {
            contentCache.put(id, content);
            logger.info(null, "ContentCacheHandlerV2:getAdminContent: fetched from content API (DB) for id: " + id);
            //redisCacheUtil.set(id, mapper.writeValueAsString(content), ttl);
            return content;
        }

        missingCache.put(id, Boolean.TRUE);
        return null;
    }
}