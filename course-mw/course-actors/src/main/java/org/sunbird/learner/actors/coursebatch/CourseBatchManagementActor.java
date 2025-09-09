package org.sunbird.learner.actors.coursebatch;

import akka.actor.ActorRef;
import akka.dispatch.Mapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.BaseRequest;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.json.JSONObject;
import org.sunbird.actor.base.BaseActor;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.Constants;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.models.util.ProjectUtil.ProgressStatus;
import org.sunbird.common.models.util.fcm.Notification;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.util.JsonUtil;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.dao.BatchUserDao;
import org.sunbird.learner.actors.coursebatch.dao.CourseBatchDao;
import org.sunbird.learner.actors.coursebatch.dao.UserCoursesDao;
import org.sunbird.learner.actors.coursebatch.dao.impl.BatchUserDaoImpl;
import org.sunbird.learner.actors.coursebatch.dao.impl.CourseBatchDaoImpl;
import org.sunbird.learner.actors.coursebatch.dao.impl.UserCoursesDaoImpl;
import org.sunbird.learner.actors.coursebatch.service.UserCoursesService;
import org.sunbird.learner.constants.CourseJsonKey;
import org.sunbird.learner.util.*;
import org.sunbird.models.batch.user.BatchUser;
import org.sunbird.models.course.batch.CourseBatch;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.userorg.UserOrgService;
import org.sunbird.userorg.UserOrgServiceImpl;
import scala.concurrent.Future;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.MediaType;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static org.sunbird.common.models.util.JsonKey.ID;
import static org.sunbird.common.models.util.JsonKey.PARTICIPANTS;

public class CourseBatchManagementActor extends BaseActor {

  private CourseBatchDao courseBatchDao = new CourseBatchDaoImpl();
  private UserOrgService userOrgService = UserOrgServiceImpl.getInstance();
  private UserCoursesService userCoursesService = new UserCoursesService();
  private ElasticSearchService esService = EsClientFactory.getInstance(JsonKey.REST);
  private String dateFormat = "yyyy-MM-dd";
  private List<String> validCourseStatus = Arrays.asList("Live", "Unlisted");
  private String timeZone = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE);
  private BatchUserDao batchUserDao = new BatchUserDaoImpl();
  private UserCoursesDao userCoursesDao = new UserCoursesDaoImpl();
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private HelperMethodService helperMethodService = new HelperMethodService();

  @Inject
  @Named("course-batch-notification-actor")
  private ActorRef courseBatchNotificationActorRef;

  @Override
  public void onReceive(Request request) throws Throwable {

    Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass().getName());

    String requestedOperation = request.getOperation();
    switch (requestedOperation) {
      case "createBatch":
        createCourseBatch(request);
        break;
      case "updateBatch":
        updateCourseBatch(request,false);
        break;
      case "getBatch":
        getCourseBatch(request);
        break;
      case "getParticipants":
        getParticipants(request);
        break;
      case "updateStartBatchesStatus":
        updateStartBatchesStatus(request);
        break;
      case "deleteBatch":
        deleteCourseBatch(request);
        break;
      default:
        onReceiveUnsupportedOperation(request.getOperation());
        break;
    }
  }

  private void createCourseBatch(Request actorMessage) throws Throwable {
    Map<String, Object> request = actorMessage.getRequest();
    Map<String, Object> targetObject;
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    String courseBatchId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
    Map<String, String> headers =
        (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);
    String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);

    if (Util.isNotNull(request.get(JsonKey.PARTICIPANTS))) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.invalidRequestParameter,
          ProjectUtil.formatMessage(
              ResponseCode.invalidRequestParameter.getErrorMessage(), PARTICIPANTS));
    }
    CourseBatch courseBatch = JsonUtil.convertFromString(request, CourseBatch.class);
    courseBatch.setStatus(setCourseBatchStatus(actorMessage.getRequestContext(), (String) request.get(JsonKey.START_DATE)));
    String courseId = (String) request.get(JsonKey.COURSE_ID);
    Map<String, Object> contentDetails = getContentDetails(actorMessage.getRequestContext(),courseId, headers);
    courseBatch.setCreatedDate(ProjectUtil.getTimeStamp());
    if(StringUtils.isBlank(courseBatch.getCreatedBy()))
    	courseBatch.setCreatedBy(requestedBy);
    validateContentOrg(actorMessage.getRequestContext(), courseBatch.getCreatedFor());
    validateMentors(courseBatch, (String) actorMessage.getContext().getOrDefault(JsonKey.X_AUTH_TOKEN, ""), actorMessage.getRequestContext());
    courseBatch.setBatchId(courseBatchId);
    String primaryCategory = (String) contentDetails.getOrDefault(JsonKey.PRIMARYCATEGORY, "");
    if (JsonKey.PRIMARY_CATEGORY_BLENDED_PROGRAM.equalsIgnoreCase(primaryCategory)) {
      if (MapUtils.isEmpty(courseBatch.getBatchAttributes()) || 
          courseBatch.getBatchAttributes().get(JsonKey.CURRENT_BATCH_SIZE) == null || 
          Integer.parseInt((String) courseBatch.getBatchAttributes().get(JsonKey.CURRENT_BATCH_SIZE)) < 1) {
            ProjectCommonException.throwClientErrorException(
              ResponseCode.currentBatchSizeInvalid, ResponseCode.currentBatchSizeInvalid.getErrorMessage());
      }
    }
    Response result = courseBatchDao.create(actorMessage.getRequestContext(), courseBatch);
    result.put(JsonKey.BATCH_ID, courseBatchId);

    Map<String, Object> esCourseMap = CourseBatchUtil.esCourseMapping(courseBatch, dateFormat);
    CourseBatchUtil.syncCourseBatchForeground(actorMessage.getRequestContext(),
        courseBatchId, esCourseMap);
    sender().tell(result, self());

    targetObject =
        TelemetryUtil.generateTargetObject(
            courseBatchId, TelemetryEnvKey.BATCH, JsonKey.CREATE, null);
    TelemetryUtil.generateCorrelatedObject(
        (String) request.get(JsonKey.COURSE_ID), JsonKey.COURSE, null, correlatedObject);

    Map<String, String> rollUp = new HashMap<>();
    rollUp.put("l1", (String) request.get(JsonKey.COURSE_ID));
    TelemetryUtil.addTargetObjectRollUp(rollUp, targetObject);
    TelemetryUtil.telemetryProcessingCall(request, targetObject, correlatedObject, actorMessage.getContext());

  //  updateBatchCount(courseBatch);
      updateCollection(actorMessage.getRequestContext(), esCourseMap, contentDetails);
    if (courseNotificationActive()) {
      batchOperationNotifier(actorMessage, courseBatch, null);
    }
  }

  private boolean courseNotificationActive() {
    return Boolean.parseBoolean(
        PropertiesCache.getInstance()
            .getProperty(JsonKey.SUNBIRD_COURSE_BATCH_NOTIFICATIONS_ENABLED));
  }

  private void batchOperationNotifier(Request actorMessage, CourseBatch courseBatch, Map<String, Object> participantMentorMap) {
    logger.debug(actorMessage.getRequestContext(), "CourseBatchManagementActor: batchoperationNotifier called");
    Request batchNotification = new Request(actorMessage.getRequestContext());
    batchNotification.getContext().putAll(actorMessage.getContext());
    batchNotification.setOperation(ActorOperations.COURSE_BATCH_NOTIFICATION.getValue());
    Map<String, Object> batchNotificationMap = new HashMap<>();
    if (participantMentorMap != null) {
      batchNotificationMap.put(JsonKey.UPDATE, true);
      batchNotificationMap.put(
          JsonKey.ADDED_MENTORS, participantMentorMap.get(JsonKey.ADDED_MENTORS));
      batchNotificationMap.put(
          JsonKey.REMOVED_MENTORS, participantMentorMap.get(JsonKey.REMOVED_MENTORS));
      batchNotificationMap.put(
          JsonKey.ADDED_PARTICIPANTS, participantMentorMap.get(JsonKey.ADDED_PARTICIPANTS));
      batchNotificationMap.put(
          JsonKey.REMOVED_PARTICIPANTS, participantMentorMap.get(JsonKey.REMOVED_PARTICIPANTS));

    } else {
      batchNotificationMap.put(JsonKey.OPERATION_TYPE, JsonKey.ADD);
      batchNotificationMap.put(JsonKey.ADDED_MENTORS, courseBatch.getMentors());
    }
    batchNotificationMap.put(JsonKey.COURSE_BATCH, courseBatch);
    batchNotification.setRequest(batchNotificationMap);
    courseBatchNotificationActorRef.tell(batchNotification, getSelf());
  }

  @SuppressWarnings("unchecked")
  private void updateCourseBatch(Request actorMessage,boolean isPrivateCall) throws Exception {
    Map<String, Object> targetObject = null;
    Map<String, Object> participantsMap = new HashMap<>();

    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    Map<String, String> headers =
            (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);
    String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);

    Map<String, Object> request = actorMessage.getRequest();
    if (Util.isNotNull(request.get(JsonKey.PARTICIPANTS))) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.invalidRequestParameter,
          ProjectUtil.formatMessage(
              ResponseCode.invalidRequestParameter.getErrorMessage(), PARTICIPANTS));
    }
    String batchId =
        request.containsKey(JsonKey.BATCH_ID)
            ? (String) request.get(JsonKey.BATCH_ID)
            : (String) request.get(JsonKey.ID);
    CourseBatch oldBatch =
        courseBatchDao.readById((String) request.get(JsonKey.COURSE_ID), batchId, actorMessage.getRequestContext());
    CourseBatch courseBatch = getUpdateCourseBatch(actorMessage.getRequestContext(), request, oldBatch,isPrivateCall);
    courseBatch.setUpdatedDate(ProjectUtil.getTimeStamp());
    Map<String, Object> contentDetails = getContentDetails(actorMessage.getRequestContext(),courseBatch.getCourseId(), headers);
    if(!isPrivateCall){
    validateUserPermission(courseBatch, requestedBy);
    validateContentOrg(actorMessage.getRequestContext(), courseBatch.getCreatedFor());
    validateMentors(courseBatch, (String) actorMessage.getContext().getOrDefault(JsonKey.X_AUTH_TOKEN, ""), actorMessage.getRequestContext());
    participantsMap = getMentorLists(participantsMap, oldBatch, courseBatch); }
    Map<String, Object> courseBatchMap = CourseBatchUtil.cassandraCourseMapping(courseBatch, dateFormat);
    Response result =
        courseBatchDao.update(actorMessage.getRequestContext(), (String) request.get(JsonKey.COURSE_ID), batchId, courseBatchMap);
    CourseBatch updatedCourseObject = mapESFieldsToObject(courseBatch);
    
    Map<String, Object> esCourseMap = CourseBatchUtil.esCourseMapping(updatedCourseObject, dateFormat);

    CourseBatchUtil.syncCourseBatchForeground(actorMessage.getRequestContext(), batchId, esCourseMap);

    targetObject =
        TelemetryUtil.generateTargetObject(batchId, TelemetryEnvKey.BATCH, JsonKey.UPDATE, null);

    Map<String, String> rollUp = new HashMap<>();
    rollUp.put("l1", courseBatch.getCourseId());
    TelemetryUtil.addTargetObjectRollUp(rollUp, targetObject);
    TelemetryUtil.telemetryProcessingCall(courseBatchMap, targetObject, correlatedObject, actorMessage.getContext());
    updateCollection(actorMessage.getRequestContext(), esCourseMap, contentDetails);

    sender().tell(result, self());
    if (courseNotificationActive()) {
      batchOperationNotifier(actorMessage, courseBatch, participantsMap);
    }
    if (batchDatesUpdateNotificationActive()) {
      batchDatesUpdateNotifier(actorMessage, courseBatch, oldBatch);
    }
  }

  private Map<String, Object> getMentorLists(
      Map<String, Object> participantsMap, CourseBatch prevBatch, CourseBatch newBatch) {
    List<String> prevMentors = prevBatch.getMentors();
    List<String> removedMentors = prevBatch.getMentors();
    List<String> addedMentors = newBatch.getMentors();

    if (addedMentors == null) {
      addedMentors = new ArrayList<>();
    }
    if (prevMentors == null) {
      prevMentors = new ArrayList<>();
      removedMentors = new ArrayList<>();
    }

    removedMentors.removeAll(addedMentors);
    addedMentors.removeAll(prevMentors);

    participantsMap.put(JsonKey.REMOVED_MENTORS, removedMentors);
    participantsMap.put(JsonKey.ADDED_MENTORS, addedMentors);

    return participantsMap;
  }

  @SuppressWarnings("unchecked")
  private CourseBatch getUpdateCourseBatch(RequestContext requestContext, Map<String, Object> request, CourseBatch oldBatch,boolean isPrivateCall) throws Exception {
    CourseBatch courseBatch = JsonUtil.deserialize(JsonUtil.serialize(oldBatch), CourseBatch.class);
    courseBatch.setEnrollmentType(
        getEnrollmentType(
            (String) request.get(JsonKey.ENROLLMENT_TYPE), courseBatch.getEnrollmentType()));
    courseBatch.setCreatedFor(
        getUpdatedCreatedFor(requestContext, 
            (List<String>) request.get(JsonKey.COURSE_CREATED_FOR),
            courseBatch.getEnrollmentType(),
            courseBatch.getCreatedFor()));

    if (request.containsKey(JsonKey.NAME)) courseBatch.setName((String) request.get(JsonKey.NAME));

    if (request.containsKey(JsonKey.DESCRIPTION))
      courseBatch.setDescription((String) request.get(JsonKey.DESCRIPTION));

    if (request.containsKey(JsonKey.MENTORS))
      courseBatch.setMentors((List<String>) request.get(JsonKey.MENTORS));

    if (request.containsKey(CourseJsonKey.BATCH_ATTRIBUTES))
      courseBatch.setBatchAttributes((Map<String, Object>) request.get(CourseJsonKey.BATCH_ATTRIBUTES));

    updateCourseBatchDate(requestContext, courseBatch, request,isPrivateCall);

    return courseBatch;
  }

  private String getEnrollmentType(String requestEnrollmentType, String dbEnrollmentType) {
    if (requestEnrollmentType != null) return requestEnrollmentType;
    return dbEnrollmentType;
  }

  private void getCourseBatch(Request actorMessage) {
    Future<Map<String, Object>> resultF =
        esService.getDataByIdentifier(
                actorMessage.getRequestContext(), ProjectUtil.EsType.courseBatch.getTypeName(),
            (String) actorMessage.getContext().get(JsonKey.BATCH_ID));
    Map<String, Object> result =
        (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(resultF);
    if (result.containsKey(JsonKey.COURSE_ID))
      result.put(JsonKey.COLLECTION_ID, result.getOrDefault(JsonKey.COURSE_ID, ""));

      if (MapUtils.isNotEmpty(result) && result.containsKey(JsonKey.STATUS)) {
          Object statusObj = result.get(JsonKey.STATUS);
          if (statusObj != null) {
              int status = Integer.parseInt(statusObj.toString());
              if (status == ProjectUtil.Status.DELETED.getValue()) {
                  ProjectCommonException.throwClientErrorException(
                          ResponseCode.resourceNotFound,   // or define a new code e.g. ResponseCode.batchDeleted
                          "This batch has been deleted and cannot be fetched."
                  );
              }
          }
      }

    Response response = new Response();
    response.put(JsonKey.RESPONSE, result);
    sender().tell(response, self());
  }


  private int setCourseBatchStatus(RequestContext requestContext, String startDate) {
    try {
      SimpleDateFormat dateFormatter = ProjectUtil.getDateFormatter(dateFormat);
      dateFormatter.setTimeZone(TimeZone.getTimeZone(timeZone));
      Date todayDate = dateFormatter.parse(dateFormatter.format(new Date()));
      Date requestedStartDate = dateFormatter.parse(startDate);
      logger.info(requestContext, "CourseBatchManagementActor:setCourseBatchStatus: todayDate="
              + todayDate + ", requestedStartDate=" + requestedStartDate);
      if (todayDate.compareTo(requestedStartDate) == 0) {
        return ProgressStatus.STARTED.getValue();
      } else {
        return ProgressStatus.NOT_STARTED.getValue();
      }
    } catch (ParseException e) {
      logger.error(requestContext, "CourseBatchManagementActor:setCourseBatchStatus: Exception occurred with error message = " + e.getMessage(), e);
    }
    return ProgressStatus.NOT_STARTED.getValue();
  }

  private void validateMentors(CourseBatch courseBatch, String authToken, RequestContext requestContext) {
    List<String> mentors = courseBatch.getMentors();
    if (CollectionUtils.isNotEmpty(mentors)) {
      mentors = mentors.stream().distinct().collect(Collectors.toList());
      courseBatch.setMentors(mentors);
      String batchCreatorRootOrgId = getRootOrg(courseBatch.getCreatedBy(), authToken);
      List<Map<String, Object>> mentorDetailList = userOrgService.getUsersByIds(mentors, authToken);
      logger.info(requestContext, "CourseBatchManagementActor::validateMentors::mentorDetailList : " + mentorDetailList);
      if (CollectionUtils.isNotEmpty(mentorDetailList)) {
        Map<String, Map<String, Object>> mentorDetails =
                mentorDetailList.stream().collect(Collectors.toMap(map -> (String) map.get(JsonKey.ID), map -> map));

        for (String mentorId : mentors) {
          Map<String, Object> result = mentorDetails.getOrDefault(mentorId, new HashMap<>());
          if (MapUtils.isEmpty(result) || Optional.ofNullable((Boolean) result.getOrDefault(JsonKey.IS_DELETED, false)).orElse(false)) {
            throw new ProjectCommonException(
                    ResponseCode.invalidUserId.getErrorCode(),
                    ResponseCode.invalidUserId.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
          } else {
            String mentorRootOrgId = getRootOrgFromUserMap(result);
            if (StringUtils.isEmpty(batchCreatorRootOrgId) || !batchCreatorRootOrgId.equals(mentorRootOrgId)) {
              throw new ProjectCommonException(
                      ResponseCode.userNotAssociatedToRootOrg.getErrorCode(),
                      ResponseCode.userNotAssociatedToRootOrg.getErrorMessage(),
                      ResponseCode.CLIENT_ERROR.getResponseCode(),
                      mentorId);
            }
          }
        }
      } else {
        logger.info(requestContext, "Invalid mentors for batchId: " + courseBatch.getBatchId() + ", mentors: " + mentors);
        throw new ProjectCommonException(
                ResponseCode.invalidUserId.getErrorCode(),
                ResponseCode.invalidUserId.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
  }

  private List<String> getUpdatedCreatedFor(
          RequestContext requestContext, List<String> createdFor, String enrolmentType, List<String> dbValueCreatedFor) {
    if (createdFor != null) {
      for (String orgId : createdFor) {
        if (!dbValueCreatedFor.contains(orgId) && !isOrgValid(requestContext, orgId)) {
          throw new ProjectCommonException(
              ResponseCode.invalidOrgId.getErrorCode(),
              ResponseCode.invalidOrgId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
      return createdFor;
    }
    return dbValueCreatedFor;
  }

  @SuppressWarnings("unchecked")
  private void updateCourseBatchDate(RequestContext requestContext, CourseBatch courseBatch, Map<String, Object> req,boolean isPrivateCall) throws Exception {
    Map<String, Object> courseBatchMap = CourseBatchUtil.cassandraCourseMapping(courseBatch, dateFormat);
    Date todayDate = getDate(requestContext, null, null);
    Date dbBatchStartDate = getDate(requestContext, JsonKey.START_DATE, courseBatchMap);
    Date dbBatchEndDate = getDate(requestContext, JsonKey.END_DATE,  courseBatchMap);
    Date dbEnrollmentEndDate = getDate(requestContext, JsonKey.ENROLLMENT_END_DATE, courseBatchMap);
    Date requestedStartDate = getDate(requestContext, JsonKey.START_DATE, req);
    Date requestedEndDate = getDate(requestContext, JsonKey.END_DATE, req);
    Date requestedEnrollmentEndDate = getDate(requestContext, JsonKey.ENROLLMENT_END_DATE, req);

    // After deprecating the text date remove below
    dbBatchStartDate = dbBatchStartDate == null ? getDate(requestContext, JsonKey.OLD_START_DATE, courseBatchMap) : dbBatchStartDate;
    dbBatchEndDate = dbBatchEndDate == null ? getDate(requestContext, JsonKey.OLD_END_DATE, courseBatchMap) : dbBatchEndDate;
    dbEnrollmentEndDate = dbEnrollmentEndDate == null ? getDate(requestContext, JsonKey.OLD_ENROLLMENT_END_DATE, courseBatchMap) : dbEnrollmentEndDate;

    if(!isPrivateCall) {
      validateUpdateBatchStartDate(requestedStartDate);
      validateBatchStartAndEndDate(
              dbBatchStartDate, dbBatchEndDate, requestedStartDate, requestedEndDate, todayDate);
    }
    /* Update the batch to In-Progress for below conditions
    * 1. StartDate is greater than or equal to today's date
    * 2. EndDate can be either NULL or 
    *     EndDate can be greater than or equal to today's date or
    *     EndDate can be greater than or equal to existing EndDate
    * */
    Boolean batchStarted = (null != requestedStartDate && todayDate.compareTo(requestedStartDate) >=0)
            && ((null == requestedEndDate) 
                || (null != requestedEndDate && null == dbBatchEndDate && todayDate.compareTo(requestedEndDate) <= 0) 
                || (null != requestedEndDate && null != dbBatchEndDate && requestedEndDate.compareTo(dbBatchEndDate) >=0));
    
    if(batchStarted)
      courseBatch.setStatus(ProgressStatus.STARTED.getValue());
    if(!isPrivateCall) {
    validateBatchEnrollmentEndDate(
        dbBatchStartDate,
        dbBatchEndDate,
        dbEnrollmentEndDate,
        requestedStartDate,
        requestedEndDate,
        requestedEnrollmentEndDate,
        todayDate); }
    courseBatch.setStartDate( 
            null != requestedStartDate
                    ? requestedStartDate
                    : courseBatch.getStartDate());
    courseBatch.setEndDate( 
             null != requestedEndDate
                    ? requestedEndDate
                    : courseBatch.getEndDate());
    courseBatch.setEnrollmentEndDate(
            null != requestedEnrollmentEndDate
                    ? requestedEnrollmentEndDate
                    : courseBatch.getEnrollmentEndDate());
  }

  private void validateUserPermission(CourseBatch courseBatch, String requestedBy) {
    List<String> canUpdateList = new ArrayList<>();
    canUpdateList.add(courseBatch.getCreatedBy());
    if (CollectionUtils.isNotEmpty(courseBatch.getMentors())) {
      canUpdateList.addAll(courseBatch.getMentors());
    }
    if (!canUpdateList.contains(requestedBy)) {
      throw new ProjectCommonException(
          ResponseCode.unAuthorized.getErrorCode(),
          ResponseCode.unAuthorized.getErrorMessage(),
          ResponseCode.UNAUTHORIZED.getResponseCode());
    }
  }

  private String getRootOrg(String batchCreator, String authToken) {

    Map<String, Object> userInfo = userOrgService.getUserById(batchCreator, authToken);
    return getRootOrgFromUserMap(userInfo);
  }

  @SuppressWarnings("unchecked")
  private String getRootOrgFromUserMap(Map<String, Object> userInfo) {
    String rootOrg = (String) userInfo.get(JsonKey.ROOT_ORG_ID);
    Map<String, Object> registeredOrgInfo =
        (Map<String, Object>) userInfo.get(JsonKey.REGISTERED_ORG);
    if (registeredOrgInfo != null && !registeredOrgInfo.isEmpty()) {
      if (null != registeredOrgInfo.get(JsonKey.IS_ROOT_ORG)
          && (Boolean) registeredOrgInfo.get(JsonKey.IS_ROOT_ORG)) {
        rootOrg = (String) registeredOrgInfo.get(JsonKey.ID);
      }
    }
    return rootOrg;
  }

  private void validateUpdateBatchStartDate(Date startDate) {
    if(null == startDate) {
      throw new ProjectCommonException(
              ResponseCode.courseBatchStartDateRequired.getErrorCode(),
              ResponseCode.courseBatchStartDateRequired.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private void validateBatchStartAndEndDate(
      Date existingStartDate,
      Date existingEndDate,
      Date requestedStartDate,
      Date requestedEndDate,
      Date todayDate) {
    Date startDate = null != requestedStartDate ? requestedStartDate : existingStartDate;
    Date endDate = null != requestedEndDate ? requestedEndDate : existingEndDate;

   /* if ((existingStartDate.before(todayDate) || existingStartDate.equals(todayDate))
        && !(existingStartDate.equals(requestedStartDate))) {
      throw new ProjectCommonException(
          ResponseCode.invalidBatchStartDateError.getErrorCode(),
          ResponseCode.invalidBatchStartDateError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    if ((requestedStartDate.before(todayDate)) && !requestedStartDate.equals(existingStartDate)) {
      throw new ProjectCommonException(
          ResponseCode.invalidBatchStartDateError.getErrorCode(),
          ResponseCode.invalidBatchStartDateError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }*/

    if (endDate != null && startDate.after(endDate)) {
      throw new ProjectCommonException(
          ResponseCode.invalidBatchEndDateError.getErrorCode(),
          ResponseCode.invalidBatchEndDateError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    
    if(null != existingEndDate && null != requestedEndDate 
            && requestedEndDate.before(existingEndDate) 
            && (null != existingEndDate && existingEndDate.before(todayDate))) {
        throw new ProjectCommonException(
                ResponseCode.courseBatchEndDateError.getErrorCode(),
                ResponseCode.courseBatchEndDateError.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private Date getDate(RequestContext requestContext, String key, Map<String, Object> map) {
    try {
      SimpleDateFormat format = ProjectUtil.getDateFormatter(dateFormat);
      format.setTimeZone(TimeZone.getTimeZone(timeZone));
      if (MapUtils.isEmpty(map)) {
        return format.parse(format.format(new Date()));
      } else {
        if (map.get(key) != null) {
          Date d;
          if (map.get(key) instanceof Date) {
            d = format.parse(format.format(map.get(key)));
          } else {
            d = format.parse((String) map.get(key));
          }
          if (key.equalsIgnoreCase(JsonKey.END_DATE) || key.equalsIgnoreCase(JsonKey.ENROLLMENT_END_DATE) ||
                  key.equalsIgnoreCase(JsonKey.OLD_END_DATE) || key.equalsIgnoreCase(JsonKey.OLD_ENROLLMENT_END_DATE)) {
            Calendar cal =
                Calendar.getInstance(
                    TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
            cal.setTime(d);
            cal.set(Calendar.HOUR_OF_DAY, 23);
            cal.set(Calendar.MINUTE, 59);
            return cal.getTime();
          }
          return d;
        } else {
          return null;
        }
      }
    } catch (ParseException e) {
      logger.error(requestContext, "CourseBatchManagementActor:getDate: Exception occurred with message = " + e.getMessage(), e);
    }
    return null;
  }

  private void validateBatchEnrollmentEndDate(
      Date existingStartDate,
      Date existingEndDate,
      Date existingEnrollmentEndDate,
      Date requestedStartDate,
      Date requestedEndDate,
      Date requestedEnrollmentEndDate,
      Date todayDate) {
    Date endDate = requestedEndDate != null ? requestedEndDate : existingEndDate;
    if (enrolmentDateValidationEnabled() && requestedEnrollmentEndDate != null
        && (requestedEnrollmentEndDate.before(requestedStartDate))) {
      throw new ProjectCommonException(
          ResponseCode.enrollmentEndDateStartError.getErrorCode(),
          ResponseCode.enrollmentEndDateStartError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (requestedEnrollmentEndDate != null
        && endDate != null
        && requestedEnrollmentEndDate.after(endDate)) {
      throw new ProjectCommonException(
          ResponseCode.enrollmentEndDateEndError.getErrorCode(),
          ResponseCode.enrollmentEndDateEndError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (requestedEnrollmentEndDate != null
        && !requestedEnrollmentEndDate.equals(existingEnrollmentEndDate)
        && requestedEnrollmentEndDate.before(todayDate)) {
      throw new ProjectCommonException(
          ResponseCode.enrollmentEndDateUpdateError.getErrorCode(),
          ResponseCode.enrollmentEndDateUpdateError.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private boolean isOrgValid(RequestContext requestContext, String orgId) {

    try {
      Map<String, Object> result = userOrgService.getOrganisationById(orgId);
      logger.debug(requestContext, "CourseBatchManagementActor:isOrgValid: orgId = "
              + (MapUtils.isNotEmpty(result) ? result.get(ID) : null));
      return ((MapUtils.isNotEmpty(result) && orgId.equals(result.get(ID))));
    } catch (Exception e) {
      logger.error(requestContext, "Error while fetching OrgID : " + orgId, e);
    }
    return false;
  }

  private Map<String, Object> getContentDetails(RequestContext requestContext, String courseId, Map<String, String> headers) {
      Map<String, Object> ekStepContent = ContentUtil.getContent(courseId, Arrays.asList(JsonKey.CONTENT_READ_REQUIRED_FIELDS.split(",")));
    logger.info(requestContext, "CourseBatchManagementActor:getEkStepContent: courseId: " + courseId, null,
            ekStepContent);
    String status = (String) ((Map<String, Object>)ekStepContent.getOrDefault("content", new HashMap<>())).getOrDefault("status", "");
    Integer leafNodesCount = (Integer) ((Map<String, Object>) ekStepContent.getOrDefault("content", new HashMap<>())).getOrDefault("leafNodesCount", 0);
    if (null == ekStepContent ||
            ekStepContent.size() == 0 ||
            !validCourseStatus.contains(status) || leafNodesCount == 0) {
      logger.info(requestContext, "CourseBatchManagementActor:getEkStepContent: Invalid courseId = " + courseId);
      throw new ProjectCommonException(
          ResponseCode.invalidCourseId.getErrorCode(),
          ResponseCode.invalidCourseId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return (Map<String, Object>)ekStepContent.getOrDefault("content", new HashMap<>());
  }

  private void validateContentOrg(RequestContext requestContext, List<String> createdFor) {
    if (createdFor != null) {
      for (String orgId : createdFor) {
        if (!isOrgValid(requestContext, orgId)) {
          throw new ProjectCommonException(
              ResponseCode.invalidOrgId.getErrorCode(),
              ResponseCode.invalidOrgId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
  }

  private void getParticipants(Request actorMessage) {
    Map<String, Object> request =
        (Map<String, Object>) actorMessage.getRequest().get(JsonKey.BATCH);
    if(null == request.get(JsonKey.ACTIVE)) {
      request.put(JsonKey.ACTIVE, true);
    }
    if(null == request.get(JsonKey.LIMIT)) {
      request.put(JsonKey.LIMIT, Constants.DEFAULT_LIMIT);
    }
    if(null == request.get(JsonKey.OFFSET)) {
      request.put(JsonKey.OFFSET, 0);
    }
    Map<String, Object> result = userCoursesService.getParticipantsListByPage(actorMessage.getRequestContext(), request);
    Response response = new Response();
    response.put(JsonKey.BATCH, result);
    sender().tell(response, self());
  }

  private CourseBatch mapESFieldsToObject(CourseBatch courseBatch) {
    Map<String, Object> certificateTemplates = courseBatch.getCertTemplates();
    if(MapUtils.isNotEmpty(certificateTemplates)) {
      certificateTemplates
              .entrySet()
              .stream()
              .forEach(
                      cert_template ->
                              certificateTemplates.put(
                                      cert_template.getKey(), mapToObject((Map<String, Object>) cert_template.getValue())));
      courseBatch.setCertTemplates(certificateTemplates);
    }
    return courseBatch;
  }

  private Map<String, Object> mapToObject(Map<String, Object> template) {
    try {
      template.put(
              JsonKey.CRITERIA,
              JsonUtil.deserialize((String) template.get(JsonKey.CRITERIA), Map.class));
      if(StringUtils.isNotEmpty((String)template.get(CourseJsonKey.SIGNATORY_LIST))) {
        template.put(
                CourseJsonKey.SIGNATORY_LIST,
                JsonUtil.deserialize((String) template.get(CourseJsonKey.SIGNATORY_LIST), ArrayList.class));
      }
      if(StringUtils.isNotEmpty((String)template.get(CourseJsonKey.ISSUER))) {
        template.put(
                CourseJsonKey.ISSUER,
                JsonUtil.deserialize((String) template.get(CourseJsonKey.ISSUER), Map.class));
      }
      if(StringUtils.isNotEmpty((String)template.get(CourseJsonKey.NOTIFY_TEMPLATE))) {
        template.put(
                CourseJsonKey.NOTIFY_TEMPLATE,
                JsonUtil.deserialize((String) template.get(CourseJsonKey.NOTIFY_TEMPLATE), Map.class));
      }
    } catch (Exception ex) {
      logger.error(null, "CourseBatchCertificateActor:mapToObject Exception occurred with error message ==", ex);
    }
    return template;
  }

  private void updateCollection(RequestContext requestContext, Map<String, Object> courseBatch, Map<String, Object> contentDetails) {
    List<Map<String, Object>> batches = (List<Map<String, Object>>) contentDetails.getOrDefault("batches", new ArrayList<>());
    Map<String, Object> data =  new HashMap<>();
    data.put("batchId", courseBatch.getOrDefault(JsonKey.BATCH_ID, ""));
    data.put("name", courseBatch.getOrDefault(JsonKey.NAME, ""));
    data.put("createdFor", courseBatch.getOrDefault(JsonKey.COURSE_CREATED_FOR, new ArrayList<>()));
    data.put("startDate", courseBatch.getOrDefault(JsonKey.START_DATE, ""));
    data.put("endDate", courseBatch.getOrDefault(JsonKey.END_DATE, null));
    data.put("startTime", courseBatch.getOrDefault(JsonKey.START_TIME, null));
    data.put("endTime", courseBatch.getOrDefault(JsonKey.END_TIME, null));
    data.put("enrollmentType", courseBatch.getOrDefault(JsonKey.ENROLLMENT_TYPE, ""));
    data.put("status", courseBatch.getOrDefault(JsonKey.STATUS, ""));
    data.put("batchAttributes", courseBatch.getOrDefault(CourseJsonKey.BATCH_ATTRIBUTES, new HashMap<String, Object>()));
    data.put("enrollmentEndDate", getEnrollmentEndDate((String) courseBatch.getOrDefault(JsonKey.ENROLLMENT_END_DATE, null), (String) courseBatch.getOrDefault(JsonKey.END_DATE, null)));
    batches.removeIf(map -> StringUtils.equalsIgnoreCase((String) courseBatch.getOrDefault(JsonKey.BATCH_ID, ""), (String) map.get("batchId")));
    batches.add(data);
    ContentUtil.updateCollection(requestContext, (String) courseBatch.getOrDefault(JsonKey.COURSE_ID, ""), new HashMap<String, Object>() {{ put("batches", batches);}});
  }

  private Object getEnrollmentEndDate(String enrollmentEndDate, String endDate) {
    SimpleDateFormat dateFormatter = ProjectUtil.getDateFormatter(dateFormat);
    dateFormatter.setTimeZone(TimeZone.getTimeZone(timeZone));
    return Optional.ofNullable(enrollmentEndDate).map(x -> x).orElse(Optional.ofNullable(endDate).map(y ->{
      Calendar cal = Calendar.getInstance();
      try {
        cal.setTime(dateFormatter.parse(y));
        cal.add(Calendar.DAY_OF_MONTH, -1);
        return dateFormatter.format(cal.getTime());
      } catch (ParseException e) {
        return null;
      }
    }).orElse(null));
  }
  private static boolean enrolmentDateValidationEnabled() {
    return Boolean.parseBoolean(
            PropertiesCache.getInstance()
                    .getProperty(JsonKey.COURSE_BATCH_ENROLL_END_DATE_LESS));
  }

  private void updateStartBatchesStatus(Request actorMessage){
    Map<String, Object> request = actorMessage.getRequest();
    String date = (String)request.get(JsonKey.START_DATE);
    if (StringUtils.isNotBlank(date) && !isValidDateFormat(date)) {
      ProjectCommonException.throwClientErrorException(
              ResponseCode.invalidRequestParameter,
              "The input date is not in the format yyyy-MM-dd");
    }
    if (StringUtils.isBlank(date)) {
      ZonedDateTime istDateTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"));
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.DATE_FORMAT);
      date = istDateTime.format(formatter);
    }
    //Construct Search DTO
    SearchDTO dto = new SearchDTO();
    Map<String, Object> filterMap = new HashMap<>();
    logger.info(actorMessage.getRequestContext(),"Current IST Time: " + date);
    HashMap<String,String> startDate = new HashMap<String,String>();
    startDate.put(Constants.EQUAL, date);
    filterMap.put(JsonKey.START_DATE,startDate);

    filterMap.put(JsonKey.STATUS,0);
    dto.getAdditionalProperties().put(JsonKey.FILTERS, filterMap);
    Future future = esService.search(actorMessage.getRequestContext(), dto, ProjectUtil.EsType.courseBatch.getTypeName());
    HashMap<String,Object> res = (HashMap<String,Object> )ElasticSearchHelper.getResponseFromFuture(future);
    if(res == null){
      logger.info(actorMessage.getRequestContext(),"No batches to update status");
      return;
    }
    ArrayList<HashMap<String,Object>> contents = (ArrayList<HashMap<String,Object>>) res.get(JsonKey.CONTENT);
    if(contents == null){
      logger.info(actorMessage.getRequestContext(),"No batches to update status");
      return;
    }
    //Headers
    HashMap<String,String> headers = new HashMap<>();
    headers.put( HttpHeaders.AUTHORIZATION, JsonKey.BEARER + System.getenv(JsonKey.SUNBIRD_AUTHORIZATION));
    headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    headers.put("Connection", "Keep-Alive");
    for(HashMap<String, Object> content:contents){
      String identifier = (String)content.get(JsonKey.IDENTIFIER);
      content.put(JsonKey.ID,identifier);
      content.remove(JsonKey.IDENTIFIER);
      Request  updateRequest =  new Request();
      updateRequest.getContext().put(JsonKey.HEADER,headers);
      updateRequest.setRequest(content);
      try {
        updateCourseBatch(updateRequest,true);
        logger.info(actorMessage.getRequestContext(),"Batch status updated for batchId :"+identifier);
      } catch (Exception e) {
        logger.error(actorMessage.getRequestContext(),"Error while updating batch "+identifier,e);
      }
    }
  }
  private static boolean isValidDateFormat(String date) {
    SimpleDateFormat sdf = new SimpleDateFormat(Constants.DATE_FORMAT);
    sdf.setLenient(false);
    try {
      sdf.parse(date);
      return true;
    } catch (ParseException e) {
      return false;
    }
  }
  private void batchDatesUpdateNotifier(Request actorMessage, CourseBatch updatedCourseBatch, CourseBatch oldCourseBatch) {
    Request batchNotification = new Request(actorMessage.getRequestContext());
    batchNotification.getContext().putAll(actorMessage.getContext());
    batchNotification.setOperation(ActorOperations.COURSE_BATCH_DATE_NOTIFICATION.getValue());
    Map<String, Object> request = new HashMap<>();
    request.put(Constants.OLD_COURSE_BATCH, oldCourseBatch);
    request.put(Constants.UPDATED_COURSE_BATCH, updatedCourseBatch);
    request.put(Constants.REQUEST_CONTEXT, actorMessage.getRequestContext());
    request.put(Constants.REQUEST_BODY, actorMessage.getRequest());
    batchNotification.setRequest(request);
    courseBatchNotificationActorRef.tell(batchNotification, getSelf());
  }

  private boolean batchDatesUpdateNotificationActive() {
    return Boolean.parseBoolean(
            PropertiesCache.getInstance()
                    .getProperty(JsonKey.SUNBIRD_BATCH_UPDATE_NOTIFICATIONS_ENABLED));
  }

    private void deleteCourseBatch(Request actorMessage) throws Exception {
        Map<String, Object> request = actorMessage.getRequest();

        String courseId = (String) request.get(JsonKey.COURSE_ID);
        String batchId =
                request.containsKey(JsonKey.BATCH_ID)
                        ? (String) request.get(JsonKey.BATCH_ID)
                        : (String) request.get(JsonKey.ID);

        CourseBatch batchDetails =
                courseBatchDao.readById(courseId, batchId, actorMessage.getRequestContext());
        if (batchDetails == null) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.resourceNotFound,
                    "Batch not found with ID: " + batchId);
        }
        if (batchDetails.getStatus() == ProjectUtil.Status.DELETED.getValue()) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.CLIENT_ERROR,
                    "Batch is already deleted: " + batchId);
        }
        Date now = ProjectUtil.getTimeStamp();
        if (batchDetails.getStartDate() != null && batchDetails.getStartDate().before(now)) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.CLIENT_ERROR,
                    "Cannot delete batch. Already started on: " + batchDetails.getStartDate());
        }
        batchDetails.setStatus(ProjectUtil.Status.DELETED.getValue());
        batchDetails.setUpdatedDate(ProjectUtil.getTimeStamp());
        Map<String, String> headers =
                (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);
        Map<String, Object> contentDetails = getContentDetails(actorMessage.getRequestContext(), courseId, headers);
        String primaryCategory = (String) contentDetails.getOrDefault(JsonKey.PRIMARYCATEGORY, "");
        if (!JsonKey.PRIMARY_CATEGORY_BLENDED_PROGRAM.equalsIgnoreCase(primaryCategory)) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.invalidRequestData,
                    "You are trying to delete the batch of primaryCategory: " + primaryCategory +
                            ", which is not allowed."
            );
        }

        Map<String, Object> courseBatchMap = CourseBatchUtil.cassandraCourseMapping(batchDetails, dateFormat);
        Response result =
                courseBatchDao.update(actorMessage.getRequestContext(), courseId, batchId, courseBatchMap);

        // Sync to ES
        CourseBatch updatedCourseObject = mapESFieldsToObject(batchDetails);
        Map<String, Object> esCourseMap = CourseBatchUtil.esCourseMapping(updatedCourseObject, dateFormat);
        CourseBatchUtil.syncCourseBatchForeground(actorMessage.getRequestContext(), batchId, esCourseMap);
        updateCollectionAfterBatchDelete(actorMessage.getRequestContext(), esCourseMap, contentDetails);
        List<String> userIds = unenrollUsersFromBatch(actorMessage.getRequestContext(), courseId, batchId,contentDetails,batchDetails);
        result.put(JsonKey.MESSAGE, "Batch deleted successfully");
        sender().tell(result, self());
        if(CollectionUtils.isNotEmpty(userIds)) {
            notifyUserUnenrollment(actorMessage.getRequestContext(), userIds, contentDetails, batchDetails, courseId);
        }
    }

    private void updateCollectionAfterBatchDelete(RequestContext requestContext, Map<String, Object> courseBatch, Map<String, Object> contentDetails) {
        List<Map<String, Object>> batches = (List<Map<String, Object>>) contentDetails.getOrDefault("batches", new ArrayList<>());
        String batchIdToDelete = (String) courseBatch.getOrDefault(JsonKey.BATCH_ID, "");
        List<Map<String, Object>> updatedBatches = batches.stream()
                .filter(batch -> !StringUtils.equalsIgnoreCase(batchIdToDelete, (String) batch.get("batchId")))
                .collect(Collectors.toList());

        ProjectLogger.log("Batch removed from collection: " + batchIdToDelete, LoggerEnum.INFO.name());

        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("batches", updatedBatches);

        ContentUtil.updateCollection(
                requestContext,
                (String) courseBatch.getOrDefault(JsonKey.COURSE_ID, ""),
                requestMap
        );
    }

    private List<String> unenrollUsersFromBatch(RequestContext ctx, String courseId, String batchId, Map<String, Object> contentDetails,CourseBatch batchDetails) throws Exception {
        // Get all users from enrollment_batch_lookup for the batch
        RequestContext requestContext = ctx;
        List<BatchUser> batchUsers = batchUserDao.readById(ctx, batchId);

        if (CollectionUtils.isEmpty(batchUsers)) {
            ProjectLogger.log("No users enrolled for batch: " + batchId, LoggerEnum.INFO.name());
            return new ArrayList<>();
        }
        List<String> userIds = new ArrayList<>();

        for (BatchUser batchUser : batchUsers) {
            String userId = batchUser.getUserId();
            Map<String, Object> data = new HashMap<>();
            data.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.INACTIVE.getValue());

            Map<String, Object> dataBatch = createBatchUserMapping(batchId, userId, batchUser, false);

            upsertEnrollment(userId, courseId, batchId, data, dataBatch, false, requestContext);

            ProjectLogger.log("Unenrolled user: " + userId + " from batch: " + batchId, LoggerEnum.INFO.name());
            userIds.add(userId);
        }
        return userIds;
    }

    public static Map<String, Object> createBatchUserMapping(String batchId, String userId, BatchUser batchUserData, boolean isActive) {
        Map<String, Object> map = new HashMap<>();

        map.put(JsonKey.BATCH_ID, batchId);
        map.put(JsonKey.USER_ID, userId);
        map.put(JsonKey.ACTIVE, isActive
                ? ProjectUtil.ActiveStatus.ACTIVE.getValue()
                : ProjectUtil.ActiveStatus.INACTIVE.getValue());

        map.put(JsonKey.COURSE_ENROLL_DATE,
                batchUserData == null ? ProjectUtil.getTimeStamp() : batchUserData.getEnrolledDate());

        return map;
    }

    public void upsertEnrollment(
            String userId,
            String courseId,
            String batchId,
            Map<String, Object> data,
            Map<String, Object> dataBatch,
            boolean isNew,
            RequestContext requestContext) throws Exception {

        Map<String, Object> dataMap = CassandraUtil.changeCassandraColumnMapping(data);
        Map<String, Object> dataBatchMap = CassandraUtil.changeCassandraColumnMapping(dataBatch);

        try {
            Object activeStatus = dataMap.get(JsonKey.ACTIVE);
            logger.info(requestContext,
                    "upsertEnrollment :: IsNew :: " + isNew +
                            " ActiveStatus :: " + activeStatus +
                            " DataMap is :: " + dataMap +
                            " DataBatchMap:: " + dataBatchMap);

            if (activeStatus == null) {
                throw new Exception("Active Value is null in upsertEnrollment");
            }
        } catch (Exception e) {
            logger.error(requestContext,
                    "Exception in upsertEnrollment list : user ::" + userId +
                            "| Exception is:" + e.getMessage(), e);
            throw e;
        }
        if (isNew) {
            userCoursesDao.insertExtendedEnrollmentV2(requestContext, dataMap);
            batchUserDao.insertBatchLookupRecord(requestContext, dataBatchMap);
        } else {
            userCoursesDao.updateExtendedEnrollV2(requestContext, userId, courseId, batchId, dataMap);
            batchUserDao.updateBatchLookupRecord(requestContext, batchId, userId, dataBatchMap, dataMap);
        }
    }

    private void notifyUserUnenrollment(RequestContext requestContext, List<String> userIds, Map<String, Object> contentDetails, CourseBatch batchDetails, String courseId) {
        List<String> emailsIds = ContentUtil.getUserEmails(userIds, requestContext);
        if (CollectionUtils.isNotEmpty(emailsIds)) {
            Map<String, Object> params = new HashMap<>();
            Map<String, Object> notificationRequest = new HashMap<>();
            Map<String, Object> action = new HashMap<>();
            Map<String, Object> usermap = new HashMap<>();
            Map<String, Object> template = new HashMap<>();

            params.put(Constants.BATCH, batchDetails.getName());
            params.put(Constants.PROGRAM, contentDetails.get(JsonKey.NAME));


            template.put(Constants.DATA, constructEmailTemplate(Constants.BATCH_DELETE_USER_NOTIFY_TEMPLATE, params, requestContext));
            template.put(Constants.ID, Constants.BATCH_DELETE_USER_NOTIFY_TEMPLATE);
            template.put(Constants.PARAMS, params);
            template.put(Constants.TYPE, Constants.EMAIL);
            usermap.put(Constants.ID, requestContext.getActorId());
            usermap.put(Constants.TYPE, Constants.USER);
            action.put(Constants.TYPE, Constants.EMAIL);
            action.put(Constants.CATEGORY, Constants.EMAIL);
            action.put(Constants.CREATED_BY, usermap);
            Map<String, Object> config = new HashMap<>();
            config.put(Constants.SUBJECT, Constants.DELETE_BATCH_MAIL_SUBJECT);
            config.put(Constants.SENDER, ProjectUtil.getConfigValue(Constants.SUPPORT_MAIL));
            template.put(Constants.CONFIG, config);
            action.put(Constants.TEMPLATE, template);
            notificationRequest.put(Constants.TYPE, Constants.EMAIL);
            notificationRequest.put(Constants.PRIORITY, 1);
            notificationRequest.put(Constants.IDS, Arrays.asList());
            notificationRequest.put(Constants.BCC_IDS, emailsIds);
            notificationRequest.put(Constants.ACTION, action);

            Map<String, Object> req = new HashMap<>();
            Map<String, List<Map<String, Object>>> notificationMap = new HashMap<>();
            notificationMap.put(Constants.NOTIFICATIONS, Collections.singletonList(notificationRequest));
            req.put(Constants.REQUEST, notificationMap);
            Notification.sendNotificationAsync(req);

            Map<String, Object> message = new HashMap<>();
            Map<String, Object> data = new HashMap<>();
            data.put(JsonKey.COURSE_ID, courseId);
            message.put(JsonKey.DATA, data);
            message.put(JsonKey.PLACE_HOLDERS, params);

            helperMethodService.sendNotification(JsonKey.DELETED_BATCH, JsonKey.ALERT, userIds, message);
        } else {
            logger.info(requestContext, "No emails found for users");
        }
    }

    private String constructEmailTemplate(String templateName, Map<String, Object> params, RequestContext requestContext) {
        String replacedHTML = new String();
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.NAME, templateName);
            List<Map<String, Object>> templateMap = null;
            Response clientResponse =
                    cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_EMAIL_TEMPLATE, propertyMap, Collections.singletonList(Constants.TEMPLATE), requestContext);
            if (null != clientResponse && !clientResponse.getResult().isEmpty()) {
                templateMap = (List<Map<String, Object>>) clientResponse.getResult().get(JsonKey.RESPONSE);
            }
            String htmlTemplate = templateMap.stream()
                    .findFirst()
                    .map(template -> (String) template.get(Constants.TEMPLATE))
                    .orElse(null);
            VelocityEngine velocityEngine = new VelocityEngine();
            velocityEngine.init();
            VelocityContext context = new VelocityContext();
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                context.put(entry.getKey(), entry.getValue());
            }
            StringWriter writer = new StringWriter();
            velocityEngine.evaluate(context, writer, "HTMLTemplate", htmlTemplate);
            replacedHTML = writer.toString();
        } catch (Exception e) {
            logger.error(requestContext, "Unable to create template ", e);
        }
        return replacedHTML;
    }
}
