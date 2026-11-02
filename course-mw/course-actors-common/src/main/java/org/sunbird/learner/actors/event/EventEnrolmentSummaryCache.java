package org.sunbird.learner.actors.event;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.cache.platform.Platform;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.learner.actors.event.impl.EventEnrolmentDaoImpl;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class EventEnrolmentSummaryCache {
    private static final Logger logger = LoggerFactory.getLogger(EventEnrolmentSummaryCache.class);
    private static final int DEFAULT_TTL_SECONDS = 3600;
    private static final String TTL_CONFIG_KEY = "user_event_enrolment_summary_cache_ttl";
    private static final String CACHE_KEY_PREFIX = "user-event-enrolment-summary:";
    static final String LOG_TAG = "[EVENT_SUMMARY_REDIS]";
    // Same settings RedisCacheUtil connects with, logged so the key can be found with redis-cli.
    private static final String REDIS_TARGET =
            Platform.getString("sunbird_redis_host", "localhost") + ":"
                    + Platform.getInteger("sunbird_redis_port", 6379)
                    + " db=" + Platform.getInteger("redis.dbIndex", 0);
    private static final EventEnrolmentSummaryCache INSTANCE =
            new EventEnrolmentSummaryCache(new RedisCacheUtil(), getConfiguredTtl());

    private final RedisCacheUtil cache;
    private final int ttlSeconds;

    EventEnrolmentSummaryCache(RedisCacheUtil cache, int ttlSeconds) {
        this.cache = cache;
        this.ttlSeconds = ttlSeconds;
        logger.info("{} op=INIT redis={} ttl={}s keyPattern={}<userId>:retired:<true|false>",
                LOG_TAG, REDIS_TARGET, ttlSeconds, CACHE_KEY_PREFIX);
    }

    public static String get(String userId, boolean retiredCoursesEnabled) {
        return INSTANCE.getCachedSummary(userId, retiredCoursesEnabled);
    }

    public static void set(String userId, boolean retiredCoursesEnabled, String value) {
        INSTANCE.putSummary(userId, retiredCoursesEnabled, value);
    }

    /**
     * Deletes the cached summaries for the user; {@code trigger} names the write path for the log.
     */
    public static void invalidate(String userId, String trigger) {
        INSTANCE.invalidateSummary(userId, trigger);
    }

    String getCachedSummary(String userId, boolean retiredCoursesEnabled) {
        String key = getCacheKey(userId, retiredCoursesEnabled);
        long start = System.currentTimeMillis();
        String value = cacheOperation("READ", key, () -> cache.get(key, null, 0), null);
        logger.info("{} op=READ result={} key={} redis={} bytes={} timeMs={}",
                LOG_TAG, StringUtils.isNotBlank(value) ? "HIT" : "MISS", key, REDIS_TARGET,
                value == null ? 0 : value.length(), System.currentTimeMillis() - start);
        return value;
    }

    void putSummary(String userId, boolean retiredCoursesEnabled, String value) {
        String key = getCacheKey(userId, retiredCoursesEnabled);
        long start = System.currentTimeMillis();
        cacheOperation("WRITE", key, () -> {
            cache.set(key, value, ttlSeconds);
            logger.info("{} op=WRITE result=OK key={} redis={} ttl={}s bytes={} timeMs={}",
                    LOG_TAG, key, REDIS_TARGET, ttlSeconds, value == null ? 0 : value.length(),
                    System.currentTimeMillis() - start);
            return null;
        }, null);
    }

    void invalidateSummary(String userId, String trigger) {
        if (StringUtils.isBlank(userId)) {
            logger.warn("{} op=DELETE result=SKIPPED reason=blankUserId trigger={}", LOG_TAG, trigger);
            return;
        }
        for (boolean retiredCoursesEnabled : new boolean[] {false, true}) {
            String key = getCacheKey(userId, retiredCoursesEnabled);
            long start = System.currentTimeMillis();
            cacheOperation("DELETE", key, () -> {
                cache.deleteKey(key);
                logger.info("{} op=DELETE result=OK key={} redis={} trigger={} timeMs={}",
                        LOG_TAG, key, REDIS_TARGET, trigger, System.currentTimeMillis() - start);
                return null;
            }, null);
        }
    }

    public static Map<String, Object> calculateSummary(
            Request request, List<Map<String, Object>> enrolledEvents, boolean retiredCoursesEnabled) {
        return calculateSummary(request, enrolledEvents, retiredCoursesEnabled, new EventEnrolmentDaoImpl());
    }

    static Map<String, Object> calculateSummary(
            Request request,
            List<Map<String, Object>> enrolledEvents,
            boolean retiredCoursesEnabled,
            EventEnrolmentDaoImpl dao) {
        int eventsCompleted = 0;
        int eventsEnrolled = 0;
        int hoursSpentOnEvents = 0;

        for (Map<String, Object> eventDetails : enrolledEvents) {
            boolean completed = Integer.valueOf(2).equals(eventDetails.get(JsonKey.STATUS));
            String eventId = (String) eventDetails.get(JsonKey.CONTENT_ID);

            if (completed) {
                eventsCompleted++;
                eventsEnrolled++;
                if (CollectionUtils.isNotEmpty((Collection<?>) eventDetails.get(JsonKey.ISSUED_CERTIFICATES))
                        && StringUtils.isNotBlank(eventId)) {
                    Map<String, Object> metadata =
                            dao.getEventDetails(request.getRequestContext(), eventId);
                    if (MapUtils.isNotEmpty(metadata) && metadata.containsKey(JsonKey.DURATION)) {
                        hoursSpentOnEvents +=
                                parseDuration(String.valueOf(metadata.get(JsonKey.DURATION))) * 60;
                    }
                }
                continue;
            }

            if (retiredCoursesEnabled) {
                eventsEnrolled++;
            } else if (StringUtils.isNotBlank(eventId)) {
                Map<String, Object> metadata =
                        dao.getEventDetails(request.getRequestContext(), eventId);
                if (MapUtils.isNotEmpty(metadata)
                        && JsonKey.LIVE.equalsIgnoreCase((String) metadata.get(JsonKey.STATUS))) {
                    eventsEnrolled++;
                }
            }
        }

        Map<String, Object> summary = new HashMap<>();
        summary.put(JsonKey.EVENTS_ENROLLED, eventsEnrolled);
        summary.put(JsonKey.EVENTS_ATTENDED, eventsCompleted);
        summary.put(JsonKey.HOURS_SPENT, hoursSpentOnEvents);
        return summary;
    }

    private static int parseDuration(String value) {
        try {
            return value.contains("E") || value.contains("e")
                    ? new BigDecimal(value).intValue()
                    : Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String getCacheKey(String userId, boolean retiredCoursesEnabled) {
        return CACHE_KEY_PREFIX + userId + ":retired:" + retiredCoursesEnabled;
    }

    private static int getConfiguredTtl() {
        String ttl = ProjectUtil.getConfigValue(TTL_CONFIG_KEY);
        try {
            return StringUtils.isNotBlank(ttl) ? Integer.parseInt(ttl.trim()) : DEFAULT_TTL_SECONDS;
        } catch (NumberFormatException e) {
            logger.warn("Invalid {} value: {}; using default {}s", TTL_CONFIG_KEY, ttl, DEFAULT_TTL_SECONDS);
            return DEFAULT_TTL_SECONDS;
        }
    }

    private static <T> T cacheOperation(
            String op, String key, CacheOperation<T> operation, T fallback) {
        try {
            return operation.run();
        } catch (Exception e) {
            logger.error(LOG_TAG + " op=" + op + " result=FAILED key=" + key + " redis=" + REDIS_TARGET
                    + " error=" + e.getMessage() + "; continuing without cache", e);
            return fallback;
        }
    }

    private interface CacheOperation<T> {
        T run();
    }
}
