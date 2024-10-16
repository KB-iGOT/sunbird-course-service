package org.sunbird.learner.actors.coursebatch;

import akka.actor.ActorRef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.base.BaseActor;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.util.JsonUtil;
import org.sunbird.learner.actors.coursebatch.dao.BatchUserDao;
import org.sunbird.learner.actors.coursebatch.dao.UserEventsDao;
import org.sunbird.learner.actors.coursebatch.dao.impl.BatchUserDaoImpl;
import org.sunbird.learner.actors.coursebatch.dao.impl.UserEventsDaoImpl;
import org.sunbird.learner.actors.eventbatch.EventBatchDao;
import org.sunbird.learner.actors.eventbatch.impl.EventBatchDaoImpl;
import org.sunbird.learner.constants.CourseJsonKey;
import org.sunbird.learner.util.ContentUtil;
import org.sunbird.learner.util.EventBatchUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.batch.user.BatchUser;
import org.sunbird.models.event.batch.EventBatch;
import org.sunbird.models.user.events.UserEvents;
import org.sunbird.redis.RedisCache;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.userorg.UserOrgService;
import org.sunbird.userorg.UserOrgServiceImpl;
import org.sunbird.common.models.util.ProjectUtil;

import java.sql.Timestamp;
import java.time.*;

import javax.inject.Inject;
import javax.inject.Named;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
    private UserEventsDao userEventsDao = new UserEventsDaoImpl();
    private BatchUserDao batchUserDao = new BatchUserDaoImpl();
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

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
            case "enrollEvent" : eventEnroll(request);
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

        Map<String, Object> esCourseMap = EventBatchUtil.esEventMapping(eventBatch, dateFormat);
        EventBatchUtil.syncEventBatchForeground(actorMessage.getRequestContext(),
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
        esCourseMap.put(JsonKey.VERSION_KEY,contentDetails.get(JsonKey.VERSION_KEY));
        updateCollection(actorMessage.getRequestContext(), esCourseMap, contentDetails);
        // if (courseNotificationActive()) {
        //    batchOperationNotifier(actorMessage, eventBatch, null);
        // }
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

    private Map<String, Object> getContentDetails(RequestContext requestContext, String eventId, Map<String, String> headers) {
        Map<String, Object> ekStepContent = ContentUtil.getContent(eventId, Arrays.asList("status", "batches", "leafNodesCount", "primaryCategory","versionKey","endDate","endTime"));
        logger.info(requestContext, "EventsActor:getEkStepContent: eventId: " + eventId, null,
                ekStepContent);
        String status = (String) ((Map<String, Object>)ekStepContent.getOrDefault("content", new HashMap<>())).getOrDefault("status", "");
        Integer leafNodesCount = (Integer) ((Map<String, Object>) ekStepContent.getOrDefault("content", new HashMap<>())).getOrDefault("leafNodesCount", 0);
        if (null == ekStepContent ||
                ekStepContent.size() == 0 ||
                !validCourseStatus.contains(status) || leafNodesCount == 0) {
            logger.info(requestContext, "EventsActor:getEkStepContent: Invalid EvetntId = " + eventId);
            throw new ProjectCommonException(
                    ResponseCode.invalidEventId.getErrorCode(),
                    ResponseCode.invalidEventId.getErrorMessage(),
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
        Map<String,Object> event = new HashMap<>();
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
        data.put("eventId",eventBatch.get(JsonKey.EVENT_ID));
        data.put("identifier",eventBatch.get(JsonKey.EVENT_ID));
        data.put("description",eventBatch.getOrDefault(JsonKey.DESCRIPTION,""));
        batches.removeIf(map -> StringUtils.equalsIgnoreCase((String) eventBatch.getOrDefault(JsonKey.BATCH_ID, ""), (String) map.get("batchId")));
        batches.add(data);
        event.put("batches", batches);
        event.put("versionKey",eventBatch.get(JsonKey.VERSION_KEY));
        event.put("identifier",eventBatch.get(JsonKey.EVENT_ID));
        ContentUtil.updateEventCollection(requestContext, (String) eventBatch.getOrDefault(JsonKey.EVENT_ID, ""), event);
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

    private void eventEnroll(Request request) throws Throwable {
        String eventId = (String) request.get(JsonKey.EVENT_ID);
        String userId = (String) request.get(JsonKey.USER_ID);
        String batchId = (String) request.get(JsonKey.BATCH_ID);

        Map<String, String> headers =
                (Map<String, String>) request.getContext().get(JsonKey.HEADER);
        Map<String, Object> contentDetails = getContentDetails(request.getRequestContext(),eventId, headers);

        EventBatch batchData = eventBatchDao.readById(eventId, batchId, request.getRequestContext());
        UserEvents enrolmentData = userEventsDao.read(request.getRequestContext(), userId, eventId, batchId);
        BatchUser batchUserData = batchUserDao.read(request.getRequestContext(), batchId, userId);

        validateEnrolment(batchData, enrolmentData, true,contentDetails);

        Map<String, Object> dataBatch = createBatchUserMapping(batchId, userId, batchUserData);
        Map<String, Object> data = createUserEnrolmentMap(
                userId, eventId, batchId, enrolmentData,
                (String) request.getContext().getOrDefault(JsonKey.REQUEST_ID, ""),
                request.getRequestContext()
        );

        boolean hasAccess = ContentUtil.getContentV4Read(
                eventId,
                (Map<String, String>) request.getContext().getOrDefault(JsonKey.HEADER, new HashMap<>())
        );

        if (hasAccess) {
            upsertEnrollment(
                    userId, eventId, batchId, data, dataBatch,
                    enrolmentData == null, request.getRequestContext()
            );

            sender().tell(successResponse(), self());
            generateTelemetryAudit(userId, eventId, batchId, data, "enrol", JsonKey.CREATE, request.getContext());
            notifyUser(userId, batchData, JsonKey.ADD);
        } else {
            throw new ProjectCommonException(ResponseCode.accessDeniedToEnrolEvent.getErrorCode(), ResponseCode.accessDeniedToEnrolEvent.getErrorMessage(),ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    private void validateEnrolment(EventBatch batchData, UserEvents enrolmentData, Boolean isEnrol,Map<String, Object> contentDetails){
        String endDateStr = (String) contentDetails.get("endDate");
        String endTimeStr = (String) contentDetails.get("endTime");

        LocalDate eventEndDate = LocalDate.parse(endDateStr);

        OffsetTime endTime = OffsetTime.parse(endTimeStr);

        LocalDateTime eventEndLocalDateTime = LocalDateTime.of(eventEndDate, endTime.toLocalTime());

        ZonedDateTime eventEndDateTime = eventEndLocalDateTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(endTime.getOffset());

        ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());

        if (now.isAfter(eventEndDateTime)) {
            ProjectCommonException.throwClientErrorException(ResponseCode.eventEnded,
                    ResponseCode.eventEnded.getErrorMessage());
        }
        if (batchData == null) {
            ProjectCommonException.throwClientErrorException(ResponseCode.invalidEventBatchId,
                    ResponseCode.invalidEventBatchId.getErrorMessage());
        }
        if (!(ProjectUtil.EnrolmentType.inviteOnly.getVal().equalsIgnoreCase(batchData.getEnrollmentType()) ||
                ProjectUtil.EnrolmentType.open.getVal().equalsIgnoreCase(batchData.getEnrollmentType()))) {
            ProjectCommonException.throwClientErrorException(ResponseCode.enrollmentTypeValidation,
                    ResponseCode.enrollmentTypeValidation.getErrorMessage());
        }

        LocalDate endDate = batchData.getEndDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate enrollmentEndDate = batchData.getEnrollmentEndDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        if (batchData.getStatus() == 2 ||
                (batchData.getEndDate() != null &&
                        LocalDateTime.now().isAfter(endDate.atStartOfDay()))) {
            ProjectCommonException.throwClientErrorException(ResponseCode.eventBatchAlreadyCompleted,
                    ResponseCode.eventBatchAlreadyCompleted.getErrorMessage());
        }

        if (isEnrol && batchData.getEnrollmentEndDate() != null &&
                LocalDateTime.now().isAfter(enrollmentEndDate.atTime(LocalTime.MAX))) {
            ProjectCommonException.throwClientErrorException(ResponseCode.EventBatchEnrollmentDateEnded,
                    ResponseCode.EventBatchEnrollmentDateEnded.getErrorMessage());
        }

        if (isEnrol && enrolmentData != null && enrolmentData.isActive()) {
            ProjectCommonException.throwClientErrorException(ResponseCode.userAlreadyEnrolledEvent,
                    ResponseCode.userAlreadyEnrolledEvent.getErrorMessage());
        }

        if (!isEnrol && (enrolmentData == null || !enrolmentData.isActive())) {
            ProjectCommonException.throwClientErrorException(ResponseCode.userNotEnrolledEvent,
                    ResponseCode.userNotEnrolledEvent.getErrorMessage());
        }

        if (!isEnrol && ProjectUtil.ProgressStatus.COMPLETED.getValue() == enrolmentData.getStatus()) {
            ProjectCommonException.throwClientErrorException(ResponseCode.eventBatchAlreadyCompleted,
                    ResponseCode.eventBatchAlreadyCompleted.getErrorMessage());
        }
    }

    public  Map<String, Object> createBatchUserMapping(String batchId, String userId, BatchUser batchUserData) {
        Map<String, Object> batchUserMap = new HashMap<>();
        batchUserMap.put(JsonKey.BATCH_ID, batchId);
        batchUserMap.put(JsonKey.USER_ID, userId);
        batchUserMap.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());

        if (batchUserData == null) {
            batchUserMap.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getTimeStamp());
        } else {
            batchUserMap.put(JsonKey.COURSE_ENROLL_DATE, batchUserData.getEnrolledDate());
        }
        return batchUserMap;
    }

    public Map<String, Object> createUserEnrolmentMap(
            String userId,
            String eventId,
            String batchId,
            UserEvents enrolmentData,
            String requestedBy,
            RequestContext requestContext) {

        Map<String, Object> enrolmentMap = new HashMap<>();

        enrolmentMap.put(JsonKey.USER_ID, userId);
        enrolmentMap.put(JsonKey.CONTENT_ID, eventId);
        enrolmentMap.put(JsonKey.CONTEXT_ID, eventId);
        enrolmentMap.put(JsonKey.BATCH_ID, batchId);
        enrolmentMap.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());

        if (enrolmentData == null) {
            enrolmentMap.put(JsonKey.ADDED_BY, requestedBy);
            enrolmentMap.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getTimeStamp());
            enrolmentMap.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
            enrolmentMap.put(JsonKey.DATE_TIME, new Timestamp(new Date().getTime()));
            enrolmentMap.put(JsonKey.COURSE_PROGRESS, 0);
        } else {
            logger.info(requestContext, "user-enrollment-null-tag, userId: "+ userId + "courseId: " + eventId+ "batchId: " + batchId);
        }

        return enrolmentMap;
    }

    public void upsertEnrollment(
            String userId,
            String eventId,
            String batchId,
            Map<String, Object> data,
            Map<String, Object> dataBatch,
            boolean isNew,
            RequestContext requestContext) throws Exception {

        // Convert the column mappings using CassandraUtil
        Map<String, Object> dataMap = CassandraUtil.changeCassandraColumnMapping(data);
        Map<String, Object> dataBatchMap = CassandraUtil.changeCassandraColumnMapping(dataBatch);

        // Debugging for potential null values
        try {
            Object activeStatus = dataMap.get(JsonKey.ACTIVE);
            Object enrolledDate = dataMap.get(JsonKey.ENROLLED_DATE);

            logger.info(requestContext,"upsertEnrollment :: IsNew DataBatchMap" );

            if (activeStatus == null) {
                throw new Exception("Active Value is null in upsertEnrollment");
            }
            if (enrolledDate == null) {
                throw new Exception("Enrolled date Value is null in upsertEnrollment");
            }
        } catch (Exception e) {
           logger.info(requestContext, "Exception in upsertEnrollment: user :: " +userId+ ". | Exception: " + e.getMessage());
            throw new Exception("Exception in upsertEnrollment: " + e);
        }

        // Perform insert or update based on 'isNew' flag
        if (isNew) {
            userEventsDao.insertV2(requestContext, dataMap);
            batchUserDao.insertBatchLookupRecord(requestContext, dataBatchMap);
        } else {
            userEventsDao.updateV2(requestContext, userId, eventId, batchId, dataMap);
            batchUserDao.updateBatchLookupRecord(requestContext, batchId, userId, dataBatchMap, dataMap);
        }
    }

    public void notifyUser(String userId, EventBatch batchData, String operationType) {
        boolean isNotifyUser = Boolean.parseBoolean(
                PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_COURSE_BATCH_NOTIFICATIONS_ENABLED)
        );
        if (isNotifyUser) {
            Request request = new Request();
            request.setOperation(ActorOperations.COURSE_BATCH_NOTIFICATION.getValue());
            request.put(JsonKey.USER_ID, userId);
            request.put(JsonKey.EVENT_BATCH, batchData);
            request.put(JsonKey.OPERATION_TYPE, operationType);

            courseBatchNotificationActorRef.tell(request, getSelf());
        }
    }

    public void generateTelemetryAudit(
            String userId,
            String eventId,
            String batchId,
            Map<String, Object> data,
            String correlation,
            String state,
            Map<String, Object> context) {

        // Create a new context map and populate it
        Map<String, Object> contextMap = new HashMap<>(context);
        contextMap.put(JsonKey.ACTOR_ID, userId);
        contextMap.put(JsonKey.ACTOR_TYPE, "User");

        // Generate the targeted object
        Map<String, Object> targetedObject = TelemetryUtil.generateTargetObject(userId, JsonKey.USER, state, null);
        Map<String, Object> rollup = new HashMap<>();
        rollup.put("l1", eventId);
        targetedObject.put(JsonKey.ROLLUP, rollup);

        // Generate correlation objects
        List<Map<String, Object>> correlationObject = new ArrayList<>();
        TelemetryUtil.generateCorrelatedObject(eventId, JsonKey.COURSE, correlation, correlationObject);
        TelemetryUtil.generateCorrelatedObject(batchId, TelemetryEnvKey.BATCH, "user.batch", correlationObject);

        // Prepare the request map
        Map<String, Object> request = new HashMap<>();
        request.put(JsonKey.USER_ID, userId);
        request.put(JsonKey.EVENT_ID, eventId);
        request.put(JsonKey.BATCH_ID, batchId);
        request.put(JsonKey.COURSE_ENROLL_DATE, data.get(JsonKey.COURSE_ENROLL_DATE));
        request.put(JsonKey.ACTIVE, data.get(JsonKey.ACTIVE));

        // Call the telemetry processing method
        TelemetryUtil.telemetryProcessingCall(request, targetedObject, correlationObject, contextMap, "enrol");
    }
}
