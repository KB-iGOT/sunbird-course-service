package org.sunbird.learner.actors.event;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.base.BaseActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.keys.SunbirdKey;
import org.sunbird.learner.actors.coursebatch.service.UserCoursesService;
import org.sunbird.learner.actors.event.impl.EventEnrolmentDaoImpl;
import org.sunbird.learner.util.Util;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

public class EventManagementActor extends BaseActor {

    private final UserCoursesService userCoursesService = new UserCoursesService();

    private EventEnrolmentDao eventBatchDao = new EventEnrolmentDaoImpl();

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
            default:
                onReceiveUnsupportedOperation(requestedOperation);
                break;
        }
    }

    private void discardEvent(Request request) throws Exception {
        validateNoEnrollments(request);
        String pathId = JsonKey.IDENTIFIER;
        String pathVal = request.getRequest().getOrDefault(JsonKey.IDENTIFIER, "").toString();
        Response response = EventContentUtil.deleteContent(request, "/private/event/v4/discard/{identifier}", pathId, pathVal);
        try {
            if (response != null && response.getResponseCode().getResponseCode() == ResponseCode.OK.getResponseCode()) {
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
                        MessageFormat.format(
                                ResponseCode.customServerError.getErrorMessage(), message));
            } else {
                ProjectCommonException.throwClientErrorException(ResponseCode.CLIENT_ERROR);
            }
        } catch (Exception ex) {
            logger.error(request.getRequestContext(), "EventManagementActor:discardEvent : discard error ", ex);
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
        List<String> participants = userCoursesService.getParticipantsList(batchId, true, request.getRequestContext());
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
            logger.error(request.getRequestContext(), "Exception in enrolment list for user: " + userId, e);
            throw e;
        }
    }

    private void getUserEventEnrollment(Request request) throws Exception {
        String userId = (String) request.get(JsonKey.USER_ID);
        String eventId = (String) request.get(JsonKey.EVENT_ID);
        String batchId = (String) request.get(JsonKey.BATCH_ID);
        logger.info(request.getRequestContext(), "EventManagementActor: list : UserId = " + userId);
        try {
            List<Map<String, Object>> result = eventBatchDao.getUserEventEnrollment(request, userId, eventId,batchId);
            Response response = new Response();
            response.put(JsonKey.EVENTS, result);
            sender().tell(response, self());
        } catch (Exception e) {
            logger.error(request.getRequestContext(), "Exception in enrolment list for user: " + userId, e);
            throw e;
        }
    }
}
