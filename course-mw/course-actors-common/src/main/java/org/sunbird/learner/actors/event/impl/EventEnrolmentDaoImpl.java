package org.sunbird.learner.actors.event.impl;



import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.event.EventEnrolmentDao;
import org.sunbird.learner.util.ContentUtil;
import java.util.*;


public class EventEnrolmentDaoImpl implements EventEnrolmentDao {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();
    public LoggerUtil logger = new LoggerUtil(this.getClass());

    @Override
    public List<Map<String, Object>> getEnrolmentList(Request request, String userId) {
        logger.info(request.getRequestContext(), "EventEnrolmentDaoImpl:getEnrolmentList: UserId = " + userId);
        List<Map<String, Object>> userEnrollmentList = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        if (userId != null && !userId.isEmpty()) {
            propertyMap.put(JsonKey.USER_ID_KEY, userId);
        }
        Response res = cassandraOperation.getRecordsByProperties(request.getRequestContext(),
                JsonKey.KEYSPACE_SUNBIRD_COURSES,
                JsonKey.TABLE_USER_EVENT_ENROLMENTS,
                propertyMap
        );
        if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
            userEnrollmentList = ((List<Map<String, Object>>) res.get(JsonKey.RESPONSE));
            for (Map<String, Object> enrollment : userEnrollmentList) {
                String eventId = (String) enrollment.get(JsonKey.EVENTID);
                String userid = (String) enrollment.get(JsonKey.USER_ID);
                String batchId = (String) enrollment.get(JsonKey.BATCH_ID);
                Map<String, Object> contentDetails = getEventDetails(request.getRequestContext(), (String) enrollment.get("eventid"));
                List<Map<String, Object>> batchDetails = getBatchList(request, eventId, batchId);
                List<Map<String, Object>> userEventConsumption = getUserEventConsumption(request, userid, batchId, eventId);
                enrollment.put("event", contentDetails);
                enrollment.put("batchDetails", batchDetails);
                enrollment.put("userEventConsumption", userEventConsumption);
            }
        }
        return userEnrollmentList;
    }

    private List<Map<String, Object>> getUserEventConsumption(Request request, String userId, String batchId, String eventId) {
        List<Map<String, Object>> userEventConsumption = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        if (userId != null && !userId.isEmpty()) {
            propertyMap.put(JsonKey.USER_ID_KEY, userId);
        }
        if (batchId != null && !batchId.isEmpty()) {
            propertyMap.put(JsonKey.BATCH_ID_KEY, batchId);
        }
        if (eventId != null && !eventId.isEmpty()) {
            propertyMap.put(JsonKey.EVENTID, eventId);
        }
        Response res = cassandraOperation.getRecordsByCompositeKey(
                JsonKey.KEYSPACE_SUNBIRD_COURSES,
                JsonKey.TABLE_USER_EVENT_CONSUMPTION,
                propertyMap,
                request.getRequestContext()
        );
        if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
            userEventConsumption = (List<Map<String, Object>>) res.getResult().get(JsonKey.RESPONSE);
        }
        return userEventConsumption;
    }

    @Override
    public List<Map<String, Object>> getBatchList(Request request, String eventId, String batchId) {
        logger.info(request.getRequestContext(), "EventEnrolmentDaoImpl:getBatchList: eventId = " + eventId + " batchId = " + batchId);
        List<Map<String, Object>> userBatchList = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        if (eventId != null && !eventId.isEmpty()) {
            propertyMap.put(JsonKey.EVENTID, eventId);
        }
        if (batchId != null && !batchId.isEmpty()) {
            propertyMap.put(JsonKey.BATCH_ID_KEY, batchId);
        }
        Response res = cassandraOperation.getRecordsByCompositeKey(
                JsonKey.KEYSPACE_SUNBIRD_COURSES,
                JsonKey.TABLE_USER_EVENT_BATCHES,
                propertyMap,
                request.getRequestContext()
        );
        if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
            userBatchList = ((List<Map<String, Object>>) res.getResult().get(JsonKey.RESPONSE));
        }
        return userBatchList;
    }

    @Override
    public List<Map<String, Object>> getUserEventEnrollment(Request request, String userId,String eventId ,String batchId){
        logger.info(request.getRequestContext(), "EventEnrolmentDaoImpl:getUserEventEnrollment: UserId = " + userId + " eventId = " + eventId + " batchId = " + batchId);
        List<Map<String, Object>> userEnrollmentList = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        if (userId != null && !userId.isEmpty()) {
            propertyMap.put(JsonKey.USER_ID_KEY, userId);
        }
        if (eventId != null && !eventId.isEmpty()) {
            propertyMap.put(JsonKey.EVENTID, eventId);
        }
        if (batchId != null && batchId.isEmpty()) {
            propertyMap.put(JsonKey.BATCH_ID_KEY, batchId);
        }
        Response res = cassandraOperation.getRecordsByCompositeKey(JsonKey.KEYSPACE_SUNBIRD_COURSES,
                JsonKey.TABLE_USER_EVENT_ENROLMENTS,
                propertyMap,
                request.getRequestContext()
        );
        if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
            userEnrollmentList = ((List<Map<String, Object>>) res.get(JsonKey.RESPONSE));
            for (Map<String, Object> enrollment : userEnrollmentList) {
                 eventId = (String) enrollment.get(JsonKey.EVENTID);
                 String userid = (String) enrollment.get(JsonKey.USER_ID);
                 batchId = (String) enrollment.get(JsonKey.BATCH_ID);
                Map<String, Object> contentDetails = getEventDetails(request.getRequestContext(), (String) enrollment.get("eventid"));
                List<Map<String, Object>> batchDetails = getBatchList(request, eventId, batchId);
                List<Map<String, Object>> userEventConsumption = getUserEventConsumption(request, userid, batchId, eventId);
                enrollment.put("event", contentDetails);
                enrollment.put("batchDetails", batchDetails);
                enrollment.put("userEventConsumption", userEventConsumption);
            }
        }
        return userEnrollmentList;
    }

    private Map<String, Object> getEventDetails(RequestContext requestContext, String eventId) {
        logger.info(requestContext, "EventEnrolmentDaoImpl:getEventDetails: eventId: " + eventId, null, null);
        Map<String, Object> response = new HashMap<>();
        try {
            String key = getCacheKey(eventId);
            int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.EVENT_REDIS_TTL));
            String cacheResponse = redisCacheUtil.get(key,null,ttl);
            ObjectMapper mapper = new ObjectMapper();
            if (cacheResponse != null && !cacheResponse.isEmpty()) {
                logger.info(requestContext, "EventEnrolmentDaoImpl:getContentDetails: Data reading from cache ", null,
                        null);
                return mapper.readValue(cacheResponse, new TypeReference<Map<String, Object>>() {});
            }else{
            Map<String, Object> ekStepContent = ContentUtil.getContent(eventId);
            logger.debug(requestContext, "EventEnrolmentDaoImpl:getContentDetails: courseId: " + eventId, null,
                    ekStepContent);
            response = (Map<String, Object>) ekStepContent.getOrDefault("content", new HashMap<>());
                redisCacheUtil.set(key, mapper.writeValueAsString(response), ttl);
            return response;
            }
        } catch (Exception e) {
            logger.error(requestContext, "Error found during event read api " + e.getMessage(), e);
        }
        return response;
    }

    private String getCacheKey(String eventId) {
        return eventId + ":user-event-enrolments";
    }

}
