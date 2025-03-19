package org.sunbird.learner.actors.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.actor.base.BaseActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.keys.SunbirdKey;
import org.sunbird.learner.actors.coursebatch.service.UserCoursesService;
import org.sunbird.learner.actors.event.impl.EventEnrolmentDaoImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.redis.RedisCache;

public class EventManagementActor extends BaseActor {

  private static final Logger log = LoggerFactory.getLogger(EventManagementActor.class);
  private final UserCoursesService userCoursesService = new UserCoursesService();

  private EventEnrolmentDao eventBatchDao = new EventEnrolmentDaoImpl();
  private RedisCache redisCache = new RedisCache();
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public void onReceive(Request request) throws Throwable {
    String requestedOperation = request.getOperation();
    switch (requestedOperation) {
      case "discardEvent":
        discardEvent(request);
        break;
      case "listEnrol":
        userEventEnrollmentList(request);
        break;
      case "getEnrol":
        getUserEventEnrollment(request);
        break;
      case "getEventState":
        getUserEventState(request);
        break;
      case "userEnrolList":
        eventEnrollmentListForUser(request);
        break;
      case "getFeatureEvent":
        getFeatureEvent(request);
        break;
      case "getTrendingEvent":
        getTrendingEvent(request);
        break;
      case "getEnrolEventSummary":
        getUserEnrolEventSummary(request);
        break;
      case "userEnrolListByEventTypes":
        eventEnrollmentListForUserBasedOnEventTypes(request);
        break;
      default:
        onReceiveUnsupportedOperation(requestedOperation);
        break;
    }
  }

  private void getUserEventState(Request request) {
    logger.info(request.getRequestContext(), "EventManagementActor: getUserEventState = ");
    try {
      List<Map<String, Object>> result = eventBatchDao.getUserEventState(request);
      Response response = new Response();
      response.put(JsonKey.EVENTS, result);
      sender().tell(response, self());
    } catch (Exception e) {
      logger.error(request.getRequestContext(), "Exception in getUserEventState for user: ", e);
      throw e;
    }
  }

  private void discardEvent(Request request) throws Exception {
    validateNoEnrollments(request);
    String pathId = JsonKey.IDENTIFIER;
    String pathVal = request.getRequest().getOrDefault(JsonKey.IDENTIFIER, "").toString();
    Response response =
        EventContentUtil.deleteContent(
            request, "/private/event/v4/discard/{identifier}", pathId, pathVal);
    try {
      if (response != null
          && response.getResponseCode().getResponseCode() == ResponseCode.OK.getResponseCode()) {
        sender().tell(response, self());
      } else if (response != null) {
        Map<String, Object> resultMap =
            Optional.ofNullable(response.getResult()).orElse(new HashMap<>());
        String message = "Event discard failed ";
        if (MapUtils.isNotEmpty(resultMap)) {
          Object obj = Optional.ofNullable(resultMap.get(SunbirdKey.TB_MESSAGES)).orElse("");
          if (obj instanceof List) {
            message += ((List<String>) obj).stream().collect(Collectors.joining(";"));
          } else if (StringUtils.isNotEmpty(response.getParams().getErrmsg())) {
            message += response.getParams().getErrmsg();
          } else {
            message += String.valueOf(obj);
          }
        }
        ProjectCommonException.throwClientErrorException(
            ResponseCode.customServerError,
            MessageFormat.format(ResponseCode.customServerError.getErrorMessage(), message));
      } else {
        ProjectCommonException.throwClientErrorException(ResponseCode.CLIENT_ERROR);
      }
    } catch (Exception ex) {
      logger.error(
          request.getRequestContext(), "EventManagementActor:discardEvent : discard error ", ex);
      if (ex instanceof ProjectCommonException) {
        throw ex;
      } else {
        throw new ProjectCommonException(
            ResponseCode.SERVER_ERROR.getErrorCode(),
            ResponseCode.SERVER_ERROR.getErrorMessage(),
            ResponseCode.SERVER_ERROR.getResponseCode());
      }
    }
  }

  private void validateNoEnrollments(Request request) {
    String identifier = request.get(SunbirdKey.IDENTIFIER).toString();
    String fixedBatchId = request.get(JsonKey.FIXED_BATCH_ID).toString();
    String batchId = Util.formBatchIdForFixedBatchId(identifier, fixedBatchId);
    List<String> participants =
        userCoursesService.getParticipantsList(batchId, true, request.getRequestContext());
    if (!participants.isEmpty()) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.cannotUpdateEventSetHavingEnrollments,
          ResponseCode.cannotUpdateEventSetHavingEnrollments.getErrorMessage());
    }
  }

  private void userEventEnrollmentList(Request request) throws Exception {
    String userId = (String) request.get(JsonKey.USER_ID);
    logger.info(request.getRequestContext(), "EventManagementActor: list : UserId = " + userId);
    try {
      List<Map<String, Object>> result = eventBatchDao.getEnrolmentList(request, userId);
      Response response = new Response();
      response.put(JsonKey.EVENTS, result);
      sender().tell(response, self());
    } catch (Exception e) {
      logger.error(
          request.getRequestContext(), "Exception in enrolment list for user: " + userId, e);
      throw e;
    }
  }

  private void eventEnrollmentListForUser(Request request) throws Exception {
    String userId = (String) request.get(JsonKey.USER_ID);
    logger.info(request.getRequestContext(), "EventManagementActor: list : UserId = " + userId);
    try {
      List<Map<String, Object>> result = eventBatchDao.getEnrolmentList(request, userId);
      Response response = new Response();
      response.put(JsonKey.EVENTS, result);
      sender().tell(response, self());
    } catch (Exception e) {
      logger.error(
          request.getRequestContext(), "Exception in enrolment list for user: " + userId, e);
      throw e;
    }
  }

  private void getUserEventEnrollment(Request request) throws Exception {
    String userId = (String) request.get(JsonKey.USER_ID);
    String eventId = (String) request.get(JsonKey.EVENT_ID);
    String batchId = (String) request.get(JsonKey.BATCH_ID);
    logger.info(request.getRequestContext(), "EventManagementActor: list : UserId = " + userId);
    try {
      List<Map<String, Object>> result =
          eventBatchDao.getUserEventEnrollment(request, userId, eventId, batchId);
      Response response = new Response();
      response.put(JsonKey.EVENTS, result);
      sender().tell(response, self());
    } catch (Exception e) {
      logger.error(
          request.getRequestContext(), "Exception in enrolment list for user: " + userId, e);
      throw e;
    }
  }

  private void getTrendingEvent(Request request) {
    String userId = (String) request.get(JsonKey.USER_ID);
    logger.info(request.getRequestContext(), "EventManagementActor: getTrendingEvent = " + userId);
    try {
      Map<String, Object> userData =
          eventBatchDao.getUserDetails(userId, request.getRequestContext());
      if (MapUtils.isEmpty(userData)) {
        log.error(
            "EventManagementActor:getTrendingEvent: UserData not found with userId: {}", userId);
        ProjectCommonException.throwServerErrorException(
            ResponseCode.RESOURCE_NOT_FOUND, "UserData not found");
      }
      String orgId = (String) userData.get(JsonKey.ROOT_ORG_ID);
      if (StringUtils.isBlank(orgId)) {
        log.error(
            "EventManagementActor:getTrendingEvent: Root orgId not found with userId: {}", userId);
        ProjectCommonException.throwServerErrorException(
            ResponseCode.invalidOrgId, "Root orgId not found");
      }

      String mapName = ProjectUtil.getConfigValue(JsonKey.TRENDING_EVENTS_REDIS_KEY);
      int dbIndex = Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.DB_INDEX));
      String eventData = redisCache.hget(mapName, orgId, dbIndex);
      if (StringUtils.isBlank(eventData)) {
        log.error(
            "EventManagementActor:getTrendingEvent: No trending events found for orgId: {}", orgId);
        Response response = new Response();
        response.put(JsonKey.MESSAGE, "No Trending events found");
        sender().tell(response, self());
        return;
      }
      List<String> eventIds = Arrays.asList(eventData.split(","));
      Response response = new Response();
      response.put(JsonKey.EVENTS, eventIds);
      sender().tell(response, self());
    } catch (Exception e) {
      logger.error(request.getRequestContext(), "Exception in eventGetTrending for user: ", e);
      ProjectCommonException.throwServerErrorException(ResponseCode.SERVER_ERROR, e.getMessage());
    }
  }

  private void getFeatureEvent(Request request) {
    logger.info(request.getRequestContext(), "EventManagementActor: getFeatureEvent ");
    try {
      String redisKey = ProjectUtil.getConfigValue(JsonKey.FEATURE_EVENTS_REDIS_KEY);
      int dbIndex = Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.DB_INDEX));
      String eventData = redisCache.getCache(redisKey, dbIndex);
      if (StringUtils.isBlank(eventData)) {
        log.error(
            "EventManagementActor:getFeatureEvent: No Feature events found for redisKey: {}",
            redisKey);
        Response response = new Response();
        response.put(JsonKey.MESSAGE, "No Feature events found");
        sender().tell(response, self());
        return;
      }
      List<String> eventIds = Arrays.asList(eventData.split(","));
      Response response = new Response();
      response.put(JsonKey.EVENTS, eventIds);
      sender().tell(response, self());
    } catch (Exception e) {
      logger.error(request.getRequestContext(), "Exception in eventGetFeature for user: ", e);
      ProjectCommonException.throwServerErrorException(ResponseCode.SERVER_ERROR, e.getMessage());
    }
  }

  private void getUserEnrolEventSummary(Request request) {
    String userId = (String) request.get(JsonKey.USER_ID);
    logger.info(
        request.getRequestContext(),
        "EventManagementActor: getUserEnrolEventSummary : UserId = " + userId);
    try {
      List<Map<String, Object>> allEnrolledEvents =
          eventBatchDao.getEventEnrolmentList(request, userId);
      Map<String, Object> userCourseEnrolmentInfo =
          getUserEnrolmentEventInfo(request, allEnrolledEvents);
      Response response = new Response();
      response.put(JsonKey.USER_EVENT_ENROLMENT_INFO, userCourseEnrolmentInfo);
      sender().tell(response, self());
    } catch (Exception e) {
      logger.error(
          request.getRequestContext(), "Exception in enrolment list for user: " + userId, e);
      throw e;
    }
  }

  private Map<String, Object> getUserEnrolmentEventInfo(
      Request request, List<Map<String, Object>> finalEnrolment) {
    int eventsCompleted = 0;
    int eventsEnrolled = 0;
    int hoursSpentOnEvents = 0;
    Map<String, Object> addInfo = new HashMap<>();

    for (Map<String, Object> eventDetails : finalEnrolment) {
      Integer eventStatus = (Integer) eventDetails.get(JsonKey.STATUS);
      List<Map<String, Object>> userEventConsumption =
          (List<Map<String, Object>>) eventDetails.get(JsonKey.USER_EVENT_CONSUMPTION);

      if (eventStatus != null && eventStatus == 2) {
        eventsCompleted++;
        eventsEnrolled++;
      } else {
        eventsEnrolled++;
      }
      int hoursSpentOnCourses = 0;
      if (userEventConsumption != null && !userEventConsumption.isEmpty()) {
        for (Map<String, Object> consumption : userEventConsumption) {
          String progressDetails = (String) consumption.get(JsonKey.PROGRESS_DETAILS);
          try {
            JsonNode progressDetailsJson = mapper.readTree(progressDetails);
            if (progressDetailsJson != null && progressDetailsJson.hasNonNull(JsonKey.DURATION)) {
              hoursSpentOnCourses += progressDetailsJson.get(JsonKey.DURATION).intValue();
            }
          } catch (Exception e) {
            logger.error(request.getRequestContext(), "Error parsing progressDetails JSON", e);
          }
        }
      }
      hoursSpentOnEvents += hoursSpentOnCourses;
    }

    addInfo.put("eventsEnrolled", eventsEnrolled);
    addInfo.put("eventsAttended", eventsCompleted);
    addInfo.put("hoursSpentOnEvents", hoursSpentOnEvents);

    return addInfo;
  }

  private void eventEnrollmentListForUserBasedOnEventTypes(Request request) throws Exception {
    String userId = (String) request.get(JsonKey.USER_ID);
    logger.info(
        request.getRequestContext(),
        "EventManagementActor: eventEnrollmentListForUserBasedOnEventTypes : UserId = " + userId);
    try {
      List<Map<String, Object>> result = eventBatchDao.getEnrolmentListV2(request, userId);
      Response response = new Response();
      response.put(JsonKey.EVENTS, result);
      sender().tell(response, self());
    } catch (Exception e) {
      logger.error(
          request.getRequestContext(),
          "Exception in eventEnrollmentListForUserBasedOnEventTypes enrolment list for user: "
              + userId,
          e);
      throw e;
    }
  }
}
