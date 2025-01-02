package org.sunbird.learner.actors.event.impl;



import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.event.EventEnrolmentDao;
import org.sunbird.learner.util.ContentUtil;
import java.util.*;
import java.util.stream.Collectors;


public class EventEnrolmentDaoImpl implements EventEnrolmentDao {

    private final Map<String, Integer> statusMap;

    public EventEnrolmentDaoImpl() {
        statusMap = buildStatusMap();
    }

    private Map<String, Integer> buildStatusMap() {
        Map<String, Integer> map = new HashMap<>();
        map.put("In-Progress", 1);
        map.put("Completed", 2);
        map.put("Not-Started", 0);
        return map;
    }

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();
    public LoggerUtil logger = new LoggerUtil(this.getClass());

    @Override
    public List<Map<String, Object>> getEnrolmentList(Request request, String userId) {
        logger.info(request.getRequestContext(), "EventEnrolmentDaoImpl:getEnrolmentList: UserId = " + userId);
        List<Map<String, Object>> userEnrollmentList = new ArrayList<>();

        Response res = cassandraOperation.getRecordsByPropertiesWithoutFiltering(request.getRequestContext(),
                JsonKey.KEYSPACE_SUNBIRD_COURSES,
                JsonKey.TABLE_USER_EVENT_ENROLMENTS,
                JsonKey.USER_ID_KEY,
                userId,
                null
        );
        String status = request.get(JsonKey.STATUS) != null ? (String)request.get(JsonKey.STATUS) : null;
        int limit = request.get(JsonKey.LIMIT) != null ? (int)request.get(JsonKey.LIMIT) : -1;
        if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
            userEnrollmentList = ((List<Map<String, Object>>) res.get(JsonKey.RESPONSE));
            if (CollectionUtils.isNotEmpty(userEnrollmentList)) {
                if (StringUtils.isNotEmpty(status) && statusMap.get(status) != null) {
                    if (statusMap.get(status) == 1) {
                        userEnrollmentList = userEnrollmentList.stream().filter(enrolment -> (int)enrolment.get(JsonKey.STATUS) != 2).collect(Collectors.toList());
                    } else {
                        userEnrollmentList = userEnrollmentList.stream().filter(enrolment -> (int)enrolment.get(JsonKey.STATUS) == statusMap.get(status)).collect(Collectors.toList());
                    }
                }
                if (limit > -1 && limit != 0) {
                    int maximumAllowedLimitForEnrolList = Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.MAXIMUM_LIMIT_ALLOWED_FOR_ENROL_LIST));
                    if (maximumAllowedLimitForEnrolList < limit) {
                        limit = maximumAllowedLimitForEnrolList;
                    }
                    userEnrollmentList = userEnrollmentList.stream()
                            .sorted(Comparator.comparing(
                                            enrolment -> (Date) ((Map<String, Object>)enrolment).get(JsonKey.LAST_CONTENT_ACCESS_TIME),
                                            Comparator.nullsLast(Comparator.naturalOrder())) // Null values last
                                    .reversed()).collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(userEnrollmentList) && userEnrollmentList.size() > limit)
                        userEnrollmentList = userEnrollmentList.subList(0, limit);
                }
            }


            for (Map<String, Object> enrollment : userEnrollmentList) {
                String contentId= (String) enrollment.get(JsonKey.CONTENT_ID);
                String contextId = (String) enrollment.get(JsonKey.CONTEXT_ID_KEY);
                String userid = (String) enrollment.get(JsonKey.USER_ID);
                String batchId = (String) enrollment.get(JsonKey.BATCH_ID);
                Map<String, Object> contentDetails = getEventDetails(request.getRequestContext(), (String) enrollment.get(JsonKey.CONTENT_ID));
                List<Map<String, Object>> batchDetails = getBatchList(request, contentId,contextId, batchId);
                List<Map<String, Object>> userEventConsumption = getUserEventConsumption(request, userid,contentId,contextId,batchId);
                enrollment.put("event", contentDetails);
                enrollment.put("batchDetails", batchDetails);
                enrollment.put("userEventConsumption", userEventConsumption);
            }
        }
        return userEnrollmentList;
    }

    private List<Map<String, Object>> getUserEventConsumption(Request request, String userId, String contentId,String contextId,String batchId) {
        List<Map<String, Object>> userEventConsumption = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        if (userId != null && !userId.isEmpty()) {
            propertyMap.put(JsonKey.USER_ID_KEY, userId);
        }
        if (contentId != null && !contentId.isEmpty()) {
            propertyMap.put(JsonKey.CONTENT_ID_KEY, contentId);
        }
        if (contextId != null && !contextId.isEmpty()) {
            propertyMap.put(JsonKey.CONTEXT_ID, contextId);
        }
        if (batchId != null && !batchId.isEmpty()) {
            propertyMap.put(JsonKey.BATCH_ID_KEY, batchId);
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

    public List<Map<String, Object>> getBatchList(Request request, String contentId,String contextId, String batchId) {
        logger.info(request.getRequestContext(), "EventEnrolmentDaoImpl:getBatchList: eventId = " + contentId + " batchId = " + batchId);
        List<Map<String, Object>> userBatchList = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();

        if (contextId != null && !contextId.isEmpty()) {
            propertyMap.put(JsonKey.EVENTID, contextId);
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
            propertyMap.put(JsonKey.CONTENT_ID_KEY, eventId);
            propertyMap.put(JsonKey.CONTEXT_ID, eventId);
        }
        if (batchId != null && batchId.isEmpty()) {
            propertyMap.put(JsonKey.BATCH_ID_KEY, batchId);
        }
        Response res = cassandraOperation.getRecordsByCompositeKey(JsonKey.KEYSPACE_SUNBIRD_COURSES,
                JsonKey.TABLE_USER_EVENT_ENROLMENTS,
                propertyMap,
                request.getRequestContext()
        );
        userEnrollmentList = ((List<Map<String, Object>>) res.get(JsonKey.RESPONSE));
        return userEnrollmentList;
    }

    @Override
    public List<Map<String, Object>> getUserEventState(Request request) {
        String userId = (String) request.get(JsonKey.USER_ID);
        String contentId = (String) request.get(JsonKey.EVENT_ID);
        String contextId = (String) request.get(JsonKey.EVENT_ID);
        String batchId = (String) request.get(JsonKey.BATCH_ID);
        List<Map<String, Object>> userEventConsumption = getUserEventConsumption(request,userId,contentId,contextId,batchId);
        return userEventConsumption;
    }

    private Map<String, Object> getEventDetails(RequestContext requestContext, String eventId) {
        logger.info(requestContext, "EventEnrolmentDaoImpl:getEventDetails: eventId: " + eventId, null, null);
        Map<String, Object> response = new HashMap<>();
        try {
            String key = getCacheKey(eventId);
            int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.EVENT_REDIS_TTL));
            String cacheResponse = redisCacheUtil.get(key,null,ttl);
            ObjectMapper mapper = new ObjectMapper();
            if (cacheResponse != null && !cacheResponse.trim().isEmpty() && !cacheResponse.trim().equals("{}")) {
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
