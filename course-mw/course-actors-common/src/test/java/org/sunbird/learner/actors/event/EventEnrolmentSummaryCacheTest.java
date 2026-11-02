package org.sunbird.learner.actors.event;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.event.impl.EventEnrolmentDaoImpl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventEnrolmentSummaryCacheTest {
    private static final String USER_ID = "user-1";
    private static final String BASE_KEY = "user-event-enrolment-summary:" + USER_ID + ":retired:";

    @Test
    public void getAndPutUseUserScopedKeyAndTtl() {
        RedisCacheUtil redisCache = mock(RedisCacheUtil.class);
        EventEnrolmentSummaryCache cache =
                new EventEnrolmentSummaryCache(redisCache, mock(EventEnrolmentDaoImpl.class));
        String cachedValue = "{\"eventsEnrolled\":1}";
        when(redisCache.get(BASE_KEY + "false", null, 0)).thenReturn(cachedValue);

        Assert.assertEquals(cachedValue, cache.getCachedSummary(USER_ID, false));
        cache.putSummary(USER_ID, true, cachedValue);

        verify(redisCache).get(BASE_KEY + "false", null, 0);
        verify(redisCache).set(BASE_KEY + "true", cachedValue, 300);
    }

    @Test
    public void refreshRecalculatesAndStoresBothRetiredCourseVariants() throws Exception {
        RedisCacheUtil redisCache = mock(RedisCacheUtil.class);
        EventEnrolmentDaoImpl dao = mock(EventEnrolmentDaoImpl.class);
        EventEnrolmentSummaryCache cache = new EventEnrolmentSummaryCache(redisCache, dao);
        Request request = mock(Request.class);

        List<Map<String, Object>> enrollments = Arrays.asList(
                enrollment("completed-event", 2, Arrays.asList("certificate")),
                enrollment("live-event", 1, null),
                enrollment("retired-event", 1, null));
        when(dao.getEventEnrolmentList(request, USER_ID)).thenReturn(enrollments);
        when(dao.getEventDetails(null, "completed-event"))
                .thenReturn(metadata(JsonKey.DURATION, "2"));
        when(dao.getEventDetails(null, "live-event"))
                .thenReturn(metadata(JsonKey.STATUS, JsonKey.LIVE));
        when(dao.getEventDetails(null, "retired-event"))
                .thenReturn(metadata(JsonKey.STATUS, JsonKey.RETIRED));

        cache.refreshSummary(request, USER_ID);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(redisCache, times(2)).set(keyCaptor.capture(), valueCaptor.capture(), eq(300));

        Assert.assertEquals(Arrays.asList(BASE_KEY + "false", BASE_KEY + "true"), keyCaptor.getAllValues());
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> standardSummary =
                mapper.readValue(valueCaptor.getAllValues().get(0), new TypeReference<Map<String, Object>>() {});
        Map<String, Object> retiredSummary =
                mapper.readValue(valueCaptor.getAllValues().get(1), new TypeReference<Map<String, Object>>() {});

        assertSummary(standardSummary, 2, 1, 120);
        assertSummary(retiredSummary, 3, 1, 120);
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
