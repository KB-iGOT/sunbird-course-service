package org.sunbird.learner.actors.event;

import org.junit.Assert;
import org.junit.Test;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.event.impl.EventEnrolmentDaoImpl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventEnrolmentSummaryCacheTest {
    private static final String USER_ID = "user-1";
    private static final String BASE_KEY = "user-event-enrolment-summary:" + USER_ID + ":retired:";

    @Test
    public void getAndPutUseUserScopedKeyAndTtl() {
        RedisCacheUtil redisCache = mock(RedisCacheUtil.class);
        EventEnrolmentSummaryCache cache =
                new EventEnrolmentSummaryCache(redisCache, 300);
        String cachedValue = "{\"eventsEnrolled\":1}";
        when(redisCache.get(BASE_KEY + "false", null, 0)).thenReturn(cachedValue);

        Assert.assertEquals(cachedValue, cache.getCachedSummary(USER_ID, false));
        cache.putSummary(USER_ID, true, cachedValue);

        verify(redisCache).get(BASE_KEY + "false", null, 0);
        verify(redisCache).set(BASE_KEY + "true", cachedValue, 300);
    }

    @Test
    public void invalidateDeletesBothRetiredCourseVariants() {
        RedisCacheUtil redisCache = mock(RedisCacheUtil.class);
        EventEnrolmentSummaryCache cache = new EventEnrolmentSummaryCache(redisCache, 300);

        cache.invalidateSummary(USER_ID, "test");

        verify(redisCache).deleteKey(BASE_KEY + "false");
        verify(redisCache).deleteKey(BASE_KEY + "true");
        verify(redisCache, never()).set(anyString(), anyString(), anyInt());
    }

    @Test
    public void calculateSummaryHonoursRetiredCoursesFlag() {
        EventEnrolmentDaoImpl dao = mock(EventEnrolmentDaoImpl.class);
        Request request = mock(Request.class);

        List<Map<String, Object>> enrollments = Arrays.asList(
                enrollment("completed-event", 2, Arrays.asList("certificate")),
                enrollment("live-event", 1, null),
                enrollment("retired-event", 1, null));
        when(dao.getEventDetails(null, "completed-event"))
                .thenReturn(metadata(JsonKey.DURATION, "2"));
        when(dao.getEventDetails(null, "live-event"))
                .thenReturn(metadata(JsonKey.STATUS, JsonKey.LIVE));
        when(dao.getEventDetails(null, "retired-event"))
                .thenReturn(metadata(JsonKey.STATUS, JsonKey.RETIRED));

        assertSummary(EventEnrolmentSummaryCache.calculateSummary(request, enrollments, false, dao), 2, 1, 120);
        assertSummary(EventEnrolmentSummaryCache.calculateSummary(request, enrollments, true, dao), 3, 1, 120);
    }

    private static Map<String, Object> enrollment(
            String eventId, int status, List<String> issuedCertificates) {
        Map<String, Object> enrollment = new HashMap<>();
        enrollment.put(JsonKey.CONTENT_ID, eventId);
        enrollment.put(JsonKey.STATUS, status);
        if (issuedCertificates != null) {
            enrollment.put(JsonKey.ISSUED_CERTIFICATES, issuedCertificates);
        }
        return enrollment;
    }

    private static Map<String, Object> metadata(String key, String value) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(key, value);
        return metadata;
    }

    private static void assertSummary(
            Map<String, Object> summary, int enrolled, int attended, int hoursSpent) {
        Assert.assertEquals(enrolled, summary.get(JsonKey.EVENTS_ENROLLED));
        Assert.assertEquals(attended, summary.get(JsonKey.EVENTS_ATTENDED));
        Assert.assertEquals(hoursSpent, summary.get(JsonKey.HOURS_SPENT));
    }
}
