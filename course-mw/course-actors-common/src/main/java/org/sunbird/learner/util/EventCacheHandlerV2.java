package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
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

/**
 * Event metadata cache, same shape as ContentCacheHandlerV2: Caffeine first, then the shared Redis
 * (LMS-owned keys "{eventId}:user-event-enrolments", written by this service), then the content read
 * API. Batched for many ids (one MGET per chunk), with a negative cache for ids nothing can resolve.
 *
 * A Redis value is accepted only if it looks like a complete event (contentType=event with start/end
 * date and time); otherwise it is refreshed from the API and rewritten — once, because the result then
 * lives in Caffeine. Previously that check ran on every request and re-fetched over HTTP each time.
 */
public class EventCacheHandlerV2 {

    static final String REDIS_KEY_SUFFIX = ":user-event-enrolments";

    private static EventCacheHandlerV2 instance;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};

    private final LoggerUtil logger = new LoggerUtil(EventCacheHandlerV2.class);
    private final RedisCacheUtil redisCacheUtil = new RedisCacheUtil();
    private final LoadingCache<String, Map<String, Object>> eventCache;
    private final Cache<String, Boolean> missingCache;
    private final int mgetBatchSize;
    private final int redisTtl;

    private EventCacheHandlerV2() {
        PropertiesCache props = PropertiesCache.getInstance();
        long ttlMinutes = intProp(props, "EVENT_CACHE_TTL_MINUTES", 30);
        long maxSize = intProp(props, "EVENT_CACHE_MAX_SIZE", 50000);
        long negativeTtl = Math.max(1, intProp(props, "content_cache_negative_ttl_seconds", 60));
        mgetBatchSize = Math.max(1, intProp(props, "content_cache_mget_batch_size", 50));
        redisTtl = intProp(props, JsonKey.EVENT_REDIS_TTL, 0);
        eventCache = Caffeine.newBuilder().maximumSize(maxSize)
                .refreshAfterWrite(Duration.ofMinutes(ttlMinutes))
                .recordStats()
                .build(new CacheLoader<String, Map<String, Object>>() {
                    @Override public Map<String, Object> load(String id) throws Exception { return loadFresh(id); }
                    @Override public Map<String, Object> reload(String id, Map<String, Object> old) {
                        try { return loadFresh(id); }            // null = source has nothing -> entry removed
                        catch (Exception e) { return old; }      // transient failure -> keep serving old
                    }
                });
        missingCache = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(Duration.ofSeconds(negativeTtl)).build();
    }

    public static EventCacheHandlerV2 getInstance() {
        if (instance == null) {
            synchronized (EventCacheHandlerV2.class) {
                if (instance == null) instance = new EventCacheHandlerV2();
            }
        }
        return instance;
    }

    public static String redisKey(String eventId) {
        return eventId + REDIS_KEY_SUFFIX;
    }

    /** Caffeine -> Redis -> content API for one event. Null when nothing resolves. */
    public Map<String, Object> getEvent(String eventId) {
        if (StringUtils.isBlank(eventId)) return null;
        Map<String, Object> hit = eventCache.getIfPresent(eventId);
        if (hit != null) {
            logger.info(null, "EventCacheHandlerV2:getEvent: local cache (Caffeine) hit for id: " + eventId);
            return hit;
        }
        Map<String, Map<String, Object>> r = getEvents(java.util.Collections.singletonList(eventId));
        return r.get(eventId);
    }

    /**
     * Batched lookup: Caffeine getAllPresent, one Redis MGET per chunk for the misses, content API only
     * for ids Redis lacks or holds in an incomplete form. Fills Caffeine (and Redis for API results).
     * Returns id -> event for ids that resolved.
     */
    public Map<String, Map<String, Object>> getEvents(Collection<String> ids) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        if (ids == null || ids.isEmpty()) return result;
        Set<String> distinct = new LinkedHashSet<>();
        for (String id : ids) if (StringUtils.isNotBlank(id)) distinct.add(id);

        result.putAll(eventCache.getAllPresent(distinct));
        if (!result.isEmpty()) {
            logger.info(null, "EventCacheHandlerV2:getEvents: local cache (Caffeine) hit for " + result.size() + " of " + distinct.size() + " ids");
        }
        List<String> misses = new ArrayList<>();
        for (String id : distinct) if (!result.containsKey(id) && missingCache.getIfPresent(id) == null) misses.add(id);
        if (misses.isEmpty()) return result;

        Map<String, String> fromRedis = new HashMap<>();
        for (int from = 0; from < misses.size(); from += mgetBatchSize) {
            List<String> chunk = misses.subList(from, Math.min(from + mgetBatchSize, misses.size()));
            List<String> keys = new ArrayList<>(chunk.size());
            for (String id : chunk) keys.add(redisKey(id));
            try {
                Map<String, String> got = redisCacheUtil.mget(keys);
                for (String id : chunk) {
                    String v = got.get(redisKey(id));
                    if (v != null) fromRedis.put(id, v);
                }
            } catch (Exception e) {
                logger.error(null, "EventCacheHandlerV2:getEvents: Redis MGET failed for a chunk of " + chunk.size() + " ids", e);
            }
        }

        List<String> stillMissing = new ArrayList<>();
        int redisHits = 0;
        for (String id : misses) {
            Map<String, Object> event = parseIfComplete(fromRedis.get(id), id);
            if (event != null) {
                eventCache.put(id, event);
                result.put(id, event);
                redisHits++;
            } else {
                stillMissing.add(id);
            }
        }
        if (redisHits > 0) {
            logger.info(null, "EventCacheHandlerV2:getEvents: cache hit (Redis) for " + redisHits + " of " + misses.size() + " ids");
        }
        if (stillMissing.isEmpty()) return result;

        logger.info(null, "EventCacheHandlerV2:getEvents: " + stillMissing.size() + " of " + distinct.size() + " ids not complete in Redis, falling back to content API (DB)");
        int apiHits = 0;
        for (String id : stillMissing) {
            Map<String, Object> event = readFromApi(id);
            if (event != null && !event.isEmpty()) {
                eventCache.put(id, event);
                result.put(id, event);
                apiHits++;
                try {
                    redisCacheUtil.set(redisKey(id), mapper.writeValueAsString(event), redisTtl);
                } catch (Exception e) {
                    logger.error(null, "EventCacheHandlerV2:getEvents: failed to write Redis key for id: " + id, e);
                }
            } else {
                missingCache.put(id, Boolean.TRUE);
            }
        }
        logger.info(null, "EventCacheHandlerV2:getEvents: content API (DB) resolved " + apiHits + " of " + stillMissing.size() + " ids");
        return result;
    }

    /** Startup pre-warm from lms_top_event_ids_key (SET or HASH of event ids on the shared Redis). Never throws. */
    public void warmFromTopEvents() {
        PropertiesCache props = PropertiesCache.getInstance();
        String key = prop(props, "lms_top_event_ids_key", "lms-top-event-ids");
        int index = intProp(props, "lms_top_event_ids_redis_index", 0);
        long pauseMs = intProp(props, "content_cache_warmup_pause_ms", 50);
        long start = System.currentTimeMillis();
        try {
            List<String> ids = redisCacheUtil.getIdsUsingIndex(key, index);
            if (ids == null || ids.isEmpty()) {
                logger.info(null, "EventCacheHandlerV2:warmFromTopEvents: " + key + " is empty, skipping pre-warm");
                return;
            }
            int loaded = 0, batches = 0;
            for (int from = 0; from < ids.size(); from += mgetBatchSize) {
                List<String> chunk = ids.subList(from, Math.min(from + mgetBatchSize, ids.size()));
                loaded += getEvents(chunk).size();
                batches += EventBatchCacheUtil.warmByEvents(chunk);   // one Cassandra partition read per event
                if (pauseMs > 0 && from + mgetBatchSize < ids.size()) Thread.sleep(pauseMs);
            }
            logger.info(null, "EventCacheHandlerV2:warmFromTopEvents: loaded " + loaded + " of " + ids.size()
                    + " events and " + batches + " event batches from " + key + " in " + (System.currentTimeMillis() - start) + " ms");
        } catch (Exception e) {
            logger.error(null, "EventCacheHandlerV2:warmFromTopEvents: pre-warm aborted after " + (System.currentTimeMillis() - start) + " ms", e);
        }
    }

    /** Redis (complete events only) -> content API for one id; no cache side effects. Null = nothing; throws on failure. */
    private Map<String, Object> loadFresh(String id) throws Exception {
        Map<String, Object> fromRedis = parseIfComplete(redisCacheUtil.get(redisKey(id), null, 0), id);
        if (fromRedis != null) return fromRedis;
        Map<String, Object> resp = ContentUtil.getContent(id);
        Object content = resp == null ? null : resp.get("content");
        if (!(content instanceof Map) || ((Map<?, ?>) content).isEmpty()) return null;
        @SuppressWarnings("unchecked") Map<String, Object> event = (Map<String, Object>) content;
        try { redisCacheUtil.set(redisKey(id), mapper.writeValueAsString(event), redisTtl); }
        catch (Exception e) { logger.error(null, "EventCacheHandlerV2:loadFresh: Redis write failed for id: " + id, e); }
        return event;
    }

    public void invalidate(String eventId) {
        if (eventId != null) { eventCache.invalidate(eventId); missingCache.invalidate(eventId); }
    }

    // ---- helpers ----

    /** A Redis value counts only if it is a complete event; otherwise null so the API path refreshes it. */
    private Map<String, Object> parseIfComplete(String json, String id) {
        if (StringUtils.isBlank(json) || "{}".equals(json.trim())) return null;
        try {
            Map<String, Object> m = mapper.readValue(json, MAP_TYPE);
            return isCompleteEvent(m) ? m : null;
        } catch (Exception e) {
            logger.error(null, "EventCacheHandlerV2: bad JSON in Redis for id: " + id, e);
            return null;
        }
    }

    static boolean isCompleteEvent(Map<String, Object> m) {
        return m != null
                && m.get("contentType") != null && "event".equalsIgnoreCase(m.get("contentType").toString())
                && m.containsKey("startDate") && m.containsKey("endDate")
                && m.containsKey("startTime") && m.containsKey("endTime");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readFromApi(String id) {
        try {
            Map<String, Object> resp = ContentUtil.getContent(id);
            Object content = resp == null ? null : resp.get("content");
            return content instanceof Map ? (Map<String, Object>) content : null;
        } catch (Exception e) {
            logger.error(null, "EventCacheHandlerV2: content API failed for id: " + id, e);
            return null;
        }
    }

    private static String prop(PropertiesCache props, String key, String def) {
        String v = props.getProperty(key);
        return (StringUtils.isBlank(v) || key.equals(v)) ? def : v.trim();
    }

    private static int intProp(PropertiesCache props, String key, int def) {
        try { return Integer.parseInt(prop(props, key, String.valueOf(def))); } catch (NumberFormatException e) { return def; }
    }
}
