package org.sunbird.learner.actors.event.impl;



import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;


public class EventEnrolmentDaoImpl implements EventEnrolmentDao {

    private static final Logger log = LoggerFactory.getLogger(EventEnrolmentDaoImpl.class);
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

    public Map<String, Object> getUserDetails(String userId, RequestContext requestContext) {
        try {
            Response response = cassandraOperation.getUserRecordFromDB(JsonKey.KEYSPACE_SUNBIRD, JsonKey.TABLE_USER, userId, requestContext);

            if (MapUtils.isEmpty(response.getResult())) {
                log.warn("No user details found for userId: {}", userId);
                return Collections.emptyMap();
            }
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, Object>> userRecords = mapper.convertValue(response.getResult().get(JsonKey.RESPONSE),
                    new TypeReference<List<Map<String, Object>>>() {});

            return CollectionUtils.isEmpty(userRecords) ? Collections.emptyMap() : userRecords.get(0);

        } catch (Exception e) {
            log.error("Exception while fetching user details for userId: {}", userId, e);
            throw new RuntimeException("Error fetching user details", e);
        }
    }

    @Override
    public List<Map<String, Object>> getEventEnrolmentList(Request request, String userId) {
        logger.info(request.getRequestContext(), "EventEnrolmentDaoImpl:getEnrolmentList: UserId = " + userId);
        List<Map<String, Object>> userEnrollmentList = new ArrayList<>();

        Response res = cassandraOperation.getRecordsByPropertiesWithoutFiltering(request.getRequestContext(),
                JsonKey.KEYSPACE_SUNBIRD_COURSES,
                JsonKey.TABLE_USER_EVENT_ENROLMENTS,
                JsonKey.USER_ID_KEY,
                userId,
                null
        );
        String status = request.get(JsonKey.STATUS) != null ? (String) request.get(JsonKey.STATUS) : null;
        int limit = request.get(JsonKey.LIMIT) != null ? (int) request.get(JsonKey.LIMIT) : -1;
        if (res != null && res.containsKey(JsonKey.RESPONSE) && res.get(JsonKey.RESPONSE) instanceof List && !((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
            userEnrollmentList = ((List<Map<String, Object>>) res.get(JsonKey.RESPONSE));
            if (CollectionUtils.isNotEmpty(userEnrollmentList)) {
                if (StringUtils.isNotEmpty(status) && statusMap.get(status) != null) {
                    if (statusMap.get(status) == 1) {
                        userEnrollmentList = userEnrollmentList.stream().filter(enrolment -> (int) enrolment.get(JsonKey.STATUS) != 2).collect(Collectors.toList());
                    } else {
                        userEnrollmentList = userEnrollmentList.stream().filter(enrolment -> (int) enrolment.get(JsonKey.STATUS) == statusMap.get(status)).collect(Collectors.toList());
                    }
                }
                if (limit > -1 && limit != 0) {
                    int maximumAllowedLimitForEnrolList = Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.MAXIMUM_LIMIT_ALLOWED_FOR_ENROL_LIST));
                    if (maximumAllowedLimitForEnrolList < limit) {
                        limit = maximumAllowedLimitForEnrolList;
                    }
                    userEnrollmentList = userEnrollmentList.stream()
                            .sorted(Comparator.comparing(
                                            enrolment -> (Date) ((Map<String, Object>) enrolment).get(JsonKey.LAST_CONTENT_ACCESS_TIME),
                                            Comparator.nullsLast(Comparator.naturalOrder())) // Null values last
                                    .reversed()).collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(userEnrollmentList) && userEnrollmentList.size() > limit)
                        userEnrollmentList = userEnrollmentList.subList(0, limit);
                }
            }


            for (Map<String, Object> enrollment : userEnrollmentList) {
                String contentId = (String) enrollment.get(JsonKey.CONTENT_ID);
                String contextId = (String) enrollment.get(JsonKey.CONTEXT_ID_KEY);
                String userid = (String) enrollment.get(JsonKey.USER_ID);
                String batchId = (String) enrollment.get(JsonKey.BATCH_ID);
                List<Map<String, Object>> userEventConsumption = getUserEventConsumption(request, userid, contentId, contextId, batchId);
                enrollment.put("userEventConsumption", userEventConsumption);
            }
        }
        return userEnrollmentList;
    }

    @Override
    public List<Map<String, Object>> getEnrolmentListV2(Request request, String userId) {
        logger.info(
                request.getRequestContext(), "EventEnrolmentDaoImpl:getEnrolmentList: UserId = " + userId);
        List<Map<String, Object>> userEnrollmentList = new ArrayList<>();
        Response res =
                cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                        request.getRequestContext(),
                        JsonKey.KEYSPACE_SUNBIRD_COURSES,
                        JsonKey.TABLE_USER_EVENT_ENROLMENTS,
                        JsonKey.USER_ID_KEY,
                        userId,
                        null);
        String status =
                request.get(JsonKey.STATUS) != null ? (String) request.get(JsonKey.STATUS) : null;
        int limit = request.get(JsonKey.LIMIT) != null ? (int) request.get(JsonKey.LIMIT) : -1;
        if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
            userEnrollmentList = ((List<Map<String, Object>>) res.get(JsonKey.RESPONSE));
            if (CollectionUtils.isNotEmpty(userEnrollmentList)) {
                if (StringUtils.isNotEmpty(status) && statusMap.get(status) != null) {
                    if (statusMap.get(status) == 1) {
                        userEnrollmentList =
                                userEnrollmentList
                                        .stream()
                                        .filter(enrolment -> (int) enrolment.get(JsonKey.STATUS) != 2)
                                        .collect(Collectors.toList());
                    } else {
                        userEnrollmentList =
                                userEnrollmentList
                                        .stream()
                                        .filter(
                                                enrolment -> (int) enrolment.get(JsonKey.STATUS) == statusMap.get(status))
                                        .collect(Collectors.toList());
                    }
                }
                if (limit > -1 && limit != 0) {
                    int maximumAllowedLimitForEnrolList =
                            Integer.parseInt(
                                    ProjectUtil.getConfigValue(JsonKey.MAXIMUM_LIMIT_ALLOWED_FOR_ENROL_LIST));
                    if (maximumAllowedLimitForEnrolList < limit) {
                        limit = maximumAllowedLimitForEnrolList;
                    }
                    userEnrollmentList =
                            userEnrollmentList
                                    .stream()
                                    .sorted(
                                            Comparator.comparing(
                                                            enrolment ->
                                                                    (Date)
                                                                            ((Map<String, Object>) enrolment)
                                                                                    .get(JsonKey.LAST_CONTENT_ACCESS_TIME),
                                                            Comparator.nullsLast(Comparator.naturalOrder())) // Null values last
                                                    .reversed())
                                    .collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(userEnrollmentList) && userEnrollmentList.size() > limit)
                        userEnrollmentList = userEnrollmentList.subList(0, limit);
                }
            }
            String requestedEventType = (String) request.get("eventType");
            for (Map<String, Object> enrollment : userEnrollmentList) {
                String contentId = (String) enrollment.get(JsonKey.CONTENT_ID);
                String contextId = (String) enrollment.get(JsonKey.CONTEXT_ID_KEY);
                String userid = (String) enrollment.get(JsonKey.USER_ID);
                String batchId = (String) enrollment.get(JsonKey.BATCH_ID);
                Map<String, Object> contentDetails =
                        getEventDetails(request.getRequestContext(), contentId);
                String actualEventType = null;
                String endDateStr = (String) request.get("eventEndDate");
                boolean calendarEventEnabled = (boolean) request.get("calendarEventEnabled");
                LocalTime endTime = LocalTime.of(0, 1);
                if (MapUtils.isNotEmpty(contentDetails)) {
                    if (MapUtils.isNotEmpty(contentDetails)) {
                        if (calendarEventEnabled) {
                            processCalendarEvent(
                                    request,
                                    enrollment,
                                    contentId,
                                    contextId,
                                    userid,
                                    batchId,
                                    contentDetails,
                                    actualEventType,
                                    requestedEventType,
                                    endDateStr);
                        } else {
                            actualEventType = determineEventType(endDateStr, endTime);
                            addEventDetailsToEnrollment(
                                    request,
                                    enrollment,
                                    contentId,
                                    contextId,
                                    userid,
                                    batchId,
                                    contentDetails,
                                    actualEventType,
                                    requestedEventType);
                        }
                    }
                }
            }
        }
        return userEnrollmentList;
    }

    private void processCalendarEvent(
            Request request,
            Map<String, Object> enrollment,
            String contentId,
            String contextId,
            String userid,
            String batchId,
            Map<String, Object> contentDetails,
            String actualEventType,
            String requestedEventType,
            String endDateStr) {
        LocalDate currentDate = LocalDate.now();
        LocalTime currentTime = LocalTime.now();
        String startDateStr = (String) request.get("eventStartDate");
        if (StringUtils.isNotEmpty(startDateStr) && StringUtils.isNotEmpty(endDateStr)) {
            LocalDate startDate = LocalDate.parse(startDateStr);
            LocalDate endDate = LocalDate.parse(endDateStr);
            if ((startDate.isBefore(currentDate) || startDate.isEqual(currentDate))
                    && (endDate.isAfter(currentDate) || endDate.isEqual(currentDate))) {
                addEventDetailsToEnrollment(
                        request,
                        enrollment,
                        contentId,
                        contextId,
                        userid,
                        batchId,
                        contentDetails,
                        actualEventType,
                        requestedEventType);
            }
        }
    }

    private String determineEventType(String endDateStr, LocalTime endTime) {
        LocalDate currentDate = LocalDate.now();
        LocalTime currentTime = LocalTime.now();
        LocalDate endDate = LocalDate.parse(endDateStr);
        if (endDate.isBefore(currentDate)
                || (endDate.isEqual(currentDate) && endTime.isBefore(currentTime))) {
            return "pastEvent";
        } else if (endDate.isEqual(currentDate) && endTime.isAfter(currentTime)) {
            return "presentEvent";
        } else {
            return "futureEvent";
        }
    }

    private void addEventDetailsToEnrollment(
            Request request,
            Map<String, Object> enrollment,
            String contentId,
            String contextId,
            String userid,
            String batchId,
            Map<String, Object> contentDetails,
            String actualEventType,
            String requestedEventType) {
        if (requestedEventType.equalsIgnoreCase(actualEventType)) {
            List<Map<String, Object>> batchDetails = getBatchList(request, contentId, contextId, batchId);
            List<Map<String, Object>> userEventConsumption =
                    getUserEventConsumption(request, userid, contentId, contextId, batchId);
            enrollment.put("event", contentDetails);
            enrollment.put("batchDetails", batchDetails);
            enrollment.put("userEventConsumption", userEventConsumption);
        }
    }
    
}
