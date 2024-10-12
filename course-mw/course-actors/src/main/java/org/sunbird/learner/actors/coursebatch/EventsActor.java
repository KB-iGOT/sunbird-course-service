package org.sunbird.learner.actors.coursebatch;

import akka.actor.ActorRef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.base.BaseActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.util.JsonUtil;
import org.sunbird.learner.actors.eventbatch.EventBatchDao;
import org.sunbird.learner.actors.eventbatch.impl.EventBatchDaoImpl;
import org.sunbird.learner.constants.CourseJsonKey;
import org.sunbird.learner.util.ContentUtil;
import org.sunbird.learner.util.CourseBatchUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.event.batch.EventBatch;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.userorg.UserOrgService;
import org.sunbird.userorg.UserOrgServiceImpl;

import javax.inject.Inject;
import javax.inject.Named;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.sunbird.common.models.util.JsonKey.ID;
import static org.sunbird.common.models.util.JsonKey.PARTICIPANTS;

public class EventsActor extends BaseActor {
    private EventBatchDao eventBatchDao = new EventBatchDaoImpl();
    private String dateFormat = "yyyy-MM-dd";
    private String timeZone = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE);
    private List<String> validCourseStatus = Arrays.asList("Live", "Unlisted");
    private UserOrgService userOrgService = UserOrgServiceImpl.getInstance();

    @Inject
    @Named("course-batch-notification-actor")
    private ActorRef courseBatchNotificationActorRef;

    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass().getName());
        String requestedOperation = request.getOperation();
        switch (requestedOperation) {
            case "createEventBatch" : createEventBatch(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    private void createEventBatch(Request actorMessage) throws Throwable {
        Map<String, Object> request = actorMessage.getRequest();
        Map<String, Object> targetObject;
        List<Map<String, Object>> correlatedObject = new ArrayList<>();
        String eventBatchId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        Map<String, String> headers =
                (Map<String, String>) actorMessage.getContext().get(JsonKey.HEADER);
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);

        if (Util.isNotNull(request.get(JsonKey.PARTICIPANTS))) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.invalidRequestParameter,
                    ProjectUtil.formatMessage(
                            ResponseCode.invalidRequestParameter.getErrorMessage(), PARTICIPANTS));
        }
        EventBatch eventBatch = JsonUtil.convertFromString(request, EventBatch.class);
        eventBatch.setStatus(setEventBatchStatus(actorMessage.getRequestContext(), (String) request.get(JsonKey.START_DATE)));
        String eventId = (String) request.get(JsonKey.EVENT_ID);
        Map<String, Object> contentDetails = getContentDetails(actorMessage.getRequestContext(),eventId, headers);
        eventBatch.setCreatedDate(ProjectUtil.getTimeStamp());
        if(StringUtils.isBlank(eventBatch.getCreatedBy()))
            eventBatch.setCreatedBy(requestedBy);
        validateContentOrg(actorMessage.getRequestContext(), eventBatch.getCreatedFor());
        validateMentors(eventBatch, (String) actorMessage.getContext().getOrDefault(JsonKey.X_AUTH_TOKEN, ""), actorMessage.getRequestContext());
        eventBatch.setBatchId(eventBatchId);
        String primaryCategory = (String) contentDetails.getOrDefault(JsonKey.PRIMARYCATEGORY, "");
        if (JsonKey.PRIMARY_CATEGORY_BLENDED_PROGRAM.equalsIgnoreCase(primaryCategory)) {
            if (MapUtils.isEmpty(eventBatch.getBatchAttributes()) ||
                    eventBatch.getBatchAttributes().get(JsonKey.CURRENT_BATCH_SIZE) == null ||
                    Integer.parseInt((String) eventBatch.getBatchAttributes().get(JsonKey.CURRENT_BATCH_SIZE)) < 1) {
                ProjectCommonException.throwClientErrorException(
                        ResponseCode.currentBatchSizeInvalid, ResponseCode.currentBatchSizeInvalid.getErrorMessage());
            }
        }
        Response result = eventBatchDao.create(actorMessage.getRequestContext(), eventBatch);
        result.put(JsonKey.BATCH_ID, eventBatchId);

        Map<String, Object> esCourseMap = CourseBatchUtil.esEventMapping(eventBatch, dateFormat);
        CourseBatchUtil.syncCourseBatchForeground(actorMessage.getRequestContext(),
                eventBatchId, esCourseMap);
        sender().tell(result, self());

        targetObject =
                TelemetryUtil.generateTargetObject(
                        eventBatchId, TelemetryEnvKey.BATCH, JsonKey.CREATE, null);
        TelemetryUtil.generateCorrelatedObject(
                (String) request.get(JsonKey.EVENT_ID), JsonKey.COURSE, null, correlatedObject);

        Map<String, String> rollUp = new HashMap<>();
        rollUp.put("l1", (String) request.get(JsonKey.EVENT_ID));
        TelemetryUtil.addTargetObjectRollUp(rollUp, targetObject);
        TelemetryUtil.telemetryProcessingCall(request, targetObject, correlatedObject, actorMessage.getContext());

        //  updateBatchCount(eventBatch);
        updateCollection(actorMessage.getRequestContext(), esCourseMap, contentDetails);
        if (courseNotificationActive()) {
            batchOperationNotifier(actorMessage, eventBatch, null);
        }
    }

    private int setEventBatchStatus(RequestContext requestContext, String startDate) {
        try {
            SimpleDateFormat dateFormatter = ProjectUtil.getDateFormatter(dateFormat);
            dateFormatter.setTimeZone(TimeZone.getTimeZone(timeZone));
            Date todayDate = dateFormatter.parse(dateFormatter.format(new Date()));
            Date requestedStartDate = dateFormatter.parse(startDate);
            logger.info(requestContext, "EventsActor:setEventBatchStatus: todayDate="
                    + todayDate + ", requestedStartDate=" + requestedStartDate);
            if (todayDate.compareTo(requestedStartDate) == 0) {
                return ProjectUtil.ProgressStatus.STARTED.getValue();
            } else {
                return ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
            }
        } catch (ParseException e) {
            logger.error(requestContext, "EventsActor:setEventBatchStatus: Exception occurred with error message = " + e.getMessage(), e);
        }
        return ProjectUtil.ProgressStatus.NOT_STARTED.getValue();
    }

    private Map<String, Object> getContentDetails(RequestContext requestContext, String courseId, Map<String, String> headers) {
        Map<String, Object> ekStepContent = ContentUtil.getContent(courseId, Arrays.asList("status", "batches", "leafNodesCount", "primaryCategory"));
        logger.info(requestContext, "EventsActor:getEkStepContent: courseId: " + courseId, null,
                ekStepContent);
        String status = (String) ((Map<String, Object>)ekStepContent.getOrDefault("content", new HashMap<>())).getOrDefault("status", "");
        Integer leafNodesCount = (Integer) ((Map<String, Object>) ekStepContent.getOrDefault("content", new HashMap<>())).getOrDefault("leafNodesCount", 0);
        if (null == ekStepContent ||
                ekStepContent.size() == 0 ||
                !validCourseStatus.contains(status) || leafNodesCount == 0) {
            logger.info(requestContext, "EventsActor:getEkStepContent: Invalid courseId = " + courseId);
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

    private void validateMentors(EventBatch eventBatch, String authToken, RequestContext requestContext) {
        List<String> mentors = eventBatch.getMentors();
        if (CollectionUtils.isNotEmpty(mentors)) {
            mentors = mentors.stream().distinct().collect(Collectors.toList());
            eventBatch.setMentors(mentors);
            String batchCreatorRootOrgId = getRootOrg(eventBatch.getCreatedBy(), authToken);
            List<Map<String, Object>> mentorDetailList = userOrgService.getUsersByIds(mentors, authToken);
            logger.info(requestContext, "EventsActor::validateMentors::mentorDetailList : " + mentorDetailList);
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
                logger.info(requestContext, "Invalid mentors for batchId: " + eventBatch.getBatchId() + ", mentors: " + mentors);
                throw new ProjectCommonException(
                        ResponseCode.invalidUserId.getErrorCode(),
                        ResponseCode.invalidUserId.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }
    }

    private boolean isOrgValid(RequestContext requestContext, String orgId) {

        try {
            Map<String, Object> result = userOrgService.getOrganisationById(orgId);
            logger.debug(requestContext, "EventsActor:isOrgValid: orgId = "
                    + (MapUtils.isNotEmpty(result) ? result.get(ID) : null));
            return ((MapUtils.isNotEmpty(result) && orgId.equals(result.get(ID))));
        } catch (Exception e) {
            logger.error(requestContext, "Error while fetching OrgID : " + orgId, e);
        }
        return false;
    }

    private String getRootOrg(String batchCreator, String authToken) {

        Map<String, Object> userInfo = userOrgService.getUserById(batchCreator, authToken);
        return getRootOrgFromUserMap(userInfo);
    }

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

    private void updateCollection(RequestContext requestContext, Map<String, Object> eventBatch, Map<String, Object> contentDetails) {
        List<Map<String, Object>> batches = (List<Map<String, Object>>) contentDetails.getOrDefault("batches", new ArrayList<>());
        Map<String, Object> data =  new HashMap<>();
        data.put("batchId", eventBatch.getOrDefault(JsonKey.BATCH_ID, ""));
        data.put("name", eventBatch.getOrDefault(JsonKey.NAME, ""));
        data.put("createdFor", eventBatch.getOrDefault(JsonKey.COURSE_CREATED_FOR, new ArrayList<>()));
        data.put("startDate", eventBatch.getOrDefault(JsonKey.START_DATE, ""));
        data.put("endDate", eventBatch.getOrDefault(JsonKey.END_DATE, null));
        data.put("startTime", eventBatch.getOrDefault(JsonKey.START_TIME, null));
        data.put("endTime", eventBatch.getOrDefault(JsonKey.END_TIME, null));
        data.put("enrollmentType", eventBatch.getOrDefault(JsonKey.ENROLLMENT_TYPE, ""));
        data.put("status", eventBatch.getOrDefault(JsonKey.STATUS, ""));
        data.put("batchAttributes", eventBatch.getOrDefault(CourseJsonKey.BATCH_ATTRIBUTES, new HashMap<String, Object>()));
        data.put("enrollmentEndDate", getEnrollmentEndDate((String) eventBatch.getOrDefault(JsonKey.ENROLLMENT_END_DATE, null), (String) eventBatch.getOrDefault(JsonKey.END_DATE, null)));
        batches.removeIf(map -> StringUtils.equalsIgnoreCase((String) eventBatch.getOrDefault(JsonKey.BATCH_ID, ""), (String) map.get("batchId")));
        batches.add(data);
        ContentUtil.updateCollection(requestContext, (String) eventBatch.getOrDefault(JsonKey.EVENT_ID, ""), new HashMap<String, Object>() {{ put("batches", batches);}});
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

    private boolean courseNotificationActive() {
        return Boolean.parseBoolean(
                PropertiesCache.getInstance()
                        .getProperty(JsonKey.SUNBIRD_COURSE_BATCH_NOTIFICATIONS_ENABLED));
    }

    private void batchOperationNotifier(Request actorMessage, EventBatch eventBatch, Map<String, Object> participantMentorMap) {
        logger.debug(actorMessage.getRequestContext(), "EventActor: batchoperationNotifier called");
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
            batchNotificationMap.put(JsonKey.ADDED_MENTORS, eventBatch.getMentors());
        }
        batchNotificationMap.put(JsonKey.COURSE_BATCH, eventBatch);
        batchNotification.setRequest(batchNotificationMap);
        courseBatchNotificationActorRef.tell(batchNotification, getSelf());
    }
}
