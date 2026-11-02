package org.sunbird.learner.actors.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.learner.actors.event.impl.EventEnrolmentDaoImpl;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class EventEnrolmentSummaryCache {
    private static final Logger logger = LoggerFactory.getLogger(EventEnrolmentSummaryCache.class);
    private static final int TTL_SECONDS = 300;
    private static final String CACHE_KEY_PREFIX = "user-event-enrolment-summary:";
    private static final EventEnrolmentSummaryCache INSTANCE =
            new EventEnrolmentSummaryCache(new RedisCacheUtil(), null);

    private final RedisCacheUtil cache;
    private final EventEnrolmentDaoImpl dao;
    private final ObjectMapper mapper = new ObjectMapper();

    EventEnrolmentSummaryCache(RedisCacheUtil cache, EventEnrolmentDaoImpl dao) {
        this.cache = cache;
        this.dao = dao;
    }

    public static String get(String userId, boolean retiredCoursesEnabled) {
        return INSTANCE.getCachedSummary(userId, retiredCoursesEnabled);
    }

    public static void set(String userId, boolean retiredCoursesEnabled, String value) {
        INSTANCE.putSummary(userId, retiredCoursesEnabled, value);
    }

    public static void refresh(Request request, String userId) {
        INSTANCE.refreshSummary(request, userId);
    }

    String getCachedSummary(String userId, boolean retiredCoursesEnabled) {
        return cacheOperation(
                () -> cache.get(getCacheKey(userId, retiredCoursesEnabled), null, 0), null);
    }

    void putSummary(String userId, boolean retiredCoursesEnabled, String value) {
        cacheOperation(() -> {
            cache.set(getCacheKey(userId, retiredCoursesEnabled), value, TTL_SECONDS);
            return null;
        }, null);
    }

    void refreshSummary(Request request, String userId) {
        try {
            EventEnrolmentDaoImpl enrolmentDao =
                    dao == null ? new EventEnrolmentDaoImpl() : dao;
            List<Map<String, Object>> enrolledEvents =
                    enrolmentDao.getEventEnrolmentList(request, userId);
            putSummary(
                    userId,
                    false,
                    mapper.writeValueAsString(
                            calculateSummary(request, enrolledEvents, false, enrolmentDao)));
            putSummary(
                    userId,
                    true,
                    mapper.writeValueAsString(
                            calculateSummary(request, enrolledEvents, true, enrolmentDao)));
        } catch (Exception e) {
            logger.warn("Unable to refresh event enrolment summary cache for user: " + userId, e);
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

    private static <T> T cacheOperation(CacheOperation<T> operation, T fallback) {
        try {
            return operation.run();
        } catch (Exception e) {
            logger.warn("Event enrolment summary cache operation failed; continuing without cache", e);
            return fallback;
        }
    }

    private interface CacheOperation<T> {
        T run();
    }
}
