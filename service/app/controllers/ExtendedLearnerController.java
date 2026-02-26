package controllers;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.keys.SunbirdKey;
import play.mvc.Http;
import play.mvc.Result;
import util.Attrs;
import util.ExtendedRequestValidator;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ExtendedLearnerController extends BaseController {


    private util.ExtendedLearnerStateRequestValidator validator = new util.ExtendedLearnerStateRequestValidator();


    @Inject
    @Named("extended-content-consumption-actor")
    private ActorRef contentConsumptionActor;


    /**
     * This method will update learner current state with last store state.
     *
     * @return Result
     */
    public CompletionStage<Result> updateContentState(Http.Request httpRequest) {
        JsonNode requestData = httpRequest.body().asJson();
        String loggingHeaders =  httpRequest.attrs().getOptional(Attrs.X_LOGGING_HEADERS).orElse(null);
        String requestedBy = httpRequest.attrs().getOptional(Attrs.USER_ID).orElse(null);
        String requestedFor = httpRequest.attrs().getOptional(Attrs.REQUESTED_FOR).orElse(null);
        String apiDebugLog = "UpdateContentState Request: " + requestData.toString() + " RequestedBy: " + requestedBy + " RequestedFor: " + requestedFor + " ";
        try {
            Request reqObj = (Request) mapper.RequestMapper.mapRequest(requestData, Request.class);
            ExtendedRequestValidator.validateUpdateContent(reqObj,false);
            reqObj = transformUserId(reqObj);
            reqObj.setOperation("updateConsumption");
            reqObj.setRequestId(httpRequest.attrs().getOptional(Attrs.REQUEST_ID).orElse(null));
            reqObj.setEnv(getEnvironment());
            HashMap<String, Object> innerMap = new HashMap<>();
            innerMap.put(JsonKey.REQUESTED_BY, requestedBy);
            if (StringUtils.isNotBlank(requestedFor))
                innerMap.put(SunbirdKey.REQUESTED_FOR, requestedFor);
            if(!reqObj.contains(JsonKey.CONTENTS) && !reqObj.contains(JsonKey.ASSESSMENT_EVENTS)) {
                innerMap.put(JsonKey.COURSE_ID, reqObj.getOrDefault(JsonKey.COURSE_ID, ""));
                innerMap.put(JsonKey.BATCH_ID, reqObj.getOrDefault(JsonKey.BATCH_ID, ""));
            } else {
                innerMap.put(JsonKey.CONTENTS, reqObj.get(JsonKey.CONTENTS));
                innerMap.put(JsonKey.ASSESSMENT_EVENTS, reqObj.getRequest().get(JsonKey.ASSESSMENT_EVENTS));
            }
            innerMap.put(JsonKey.USER_ID, reqObj.getRequest().get(JsonKey.USER_ID));
            reqObj.setRequest(innerMap);
            CompletionStage<Result> result = actorResponseHandler(contentConsumptionActor, reqObj, timeout, null, httpRequest);
            return result.thenApplyAsync(r -> {
                logger.info(null,apiDebugLog + ":: ResponseStatus: " + r.status() + " Headers: " + loggingHeaders);
                return r;
            });
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest)).thenApplyAsync(r -> {
                logger.info(null,apiDebugLog + ":: ResponseStatus: " + r.status() + " Headers: " + loggingHeaders +  " ErrMessage: " + e.getMessage());
                return r;
            });
        }
    }

    /**
     * This method will provide list of user content state. Content refer user activity {started,half
     * completed ,completed} against TOC (table of content).
     *
     * @return Result
     */
    public CompletionStage<Result> privateGetContentState(Http.Request httpRequest) {
        try {
            JsonNode requestJson = httpRequest.body().asJson();
            Request request =
                    createAndInitRequest("getConsumption", requestJson, httpRequest);
            validator.validateGetContentState(request);
            request = transformUserId(request);
            return actorResponseHandler(
                    contentConsumptionActor, request, timeout, JsonKey.CONTENT_LIST, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    /**
     * This method will provide list of user content state. Content refer user activity {started,half
     * completed ,completed} against TOC (table of content).
     *
     * @return Result
     */
    public CompletionStage<Result> getContentState(Http.Request httpRequest) {
        try {
            Map<String, Object> contextMap = new HashMap<>();
            JsonNode requestJson = httpRequest.body().asJson();
            Request request =
                    createAndInitRequest("getConsumption", requestJson, httpRequest);
            String userIdb = requestJson.get(JsonKey.REQUEST).get("userId").asText();
            request.getContext().put(JsonKey.REQUESTED_BY, userIdb);
            request.getContext().put(JsonKey.REQUESTED_FOR, userIdb);
            String userId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
            validator.validateRequestedBy(userId);
            request.getRequest().put(JsonKey.USER_ID, userId);
            validator.validateGetContentState(request);
            request = transformUserId(request);
            return actorResponseHandler(
                    contentConsumptionActor, request, timeout, JsonKey.CONTENT_LIST, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    public CompletionStage<Result> updateContentStateByAdmin(Http.Request httpRequest) {
        JsonNode requestData = httpRequest.body().asJson();
        String loggingHeaders =  httpRequest.attrs().getOptional(Attrs.X_LOGGING_HEADERS).orElse(null);
        String requestedBy = httpRequest.attrs().getOptional(Attrs.USER_ID).orElse(null);
        String apiDebugLog = "UpdateContentState Request: " + requestData.toString() + " RequestedBy: " + requestedBy;
        try {
            Request reqObj = (Request) mapper.RequestMapper.mapRequest(requestData, Request.class);
            String requestedFor = (String) reqObj.getRequest().getOrDefault(JsonKey.USER_ID, null);
            ExtendedRequestValidator.validateUpdateContent(reqObj, true);
            reqObj = transformUserId(reqObj);
            reqObj.setOperation("updateConsumption");
            reqObj.setRequestId(httpRequest.attrs().getOptional(Attrs.REQUEST_ID).orElse(null));
            reqObj.setEnv(getEnvironment());
            HashMap<String, Object> innerMap = new HashMap<>();
            innerMap.put(JsonKey.REQUESTED_BY, requestedBy);
            if (StringUtils.isNotBlank(requestedFor))
                innerMap.put(SunbirdKey.REQUESTED_FOR, requestedFor);
            if(!reqObj.contains(JsonKey.CONTENTS) && !reqObj.contains(JsonKey.ASSESSMENT_EVENTS)) {
                innerMap.put(JsonKey.COURSE_ID, reqObj.getOrDefault(JsonKey.COURSE_ID, ""));
                innerMap.put(JsonKey.BATCH_ID, reqObj.getOrDefault(JsonKey.BATCH_ID, ""));
            } else {
                innerMap.put(JsonKey.CONTENTS, reqObj.get(JsonKey.CONTENTS));
                innerMap.put(JsonKey.ASSESSMENT_EVENTS, reqObj.getRequest().get(JsonKey.ASSESSMENT_EVENTS));
            }
            innerMap.put(JsonKey.USER_ID, reqObj.getRequest().get(JsonKey.USER_ID));
            reqObj.setRequest(innerMap);
            CompletionStage<Result> result = actorResponseHandler(contentConsumptionActor, reqObj, timeout, null, httpRequest);
            return result.thenApplyAsync(r -> {
                logger.info(null,apiDebugLog + ":: ResponseStatus: " + r.status() + " Headers: " + loggingHeaders);
                return r;
            });
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest)).thenApplyAsync(r -> {
                logger.info(null,apiDebugLog + ":: ResponseStatus: " + r.status() + " Headers: " + loggingHeaders +  " ErrMessage: " + e.getMessage());
                return r;
            });
        }
    }


}
