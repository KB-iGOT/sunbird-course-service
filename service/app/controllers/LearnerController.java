/** */
package controllers;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.LearnerStateRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.keys.SunbirdKey;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;
import util.Attrs;
import util.RequestValidator;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * This controller will handler all the request related to learner state.
 *
 * @author Manzarul
 */
public class LearnerController extends BaseController {

  private LearnerStateRequestValidator validator = new LearnerStateRequestValidator();

  @Inject
  @Named("content-consumption-actor")
  private ActorRef contentConsumptionActor;

  /**
   * This method will provide list of user content state. Content refer user activity {started,half
   * completed ,completed} against TOC (table of content).
   *
   * @return Result
   */
  public CompletionStage<Result> getContentState(Http.Request httpRequest) {
    try {
      JsonNode requestJson = httpRequest.body().asJson();
      Request request =
          createAndInitRequest("getConsumption", requestJson, httpRequest);
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
      RequestValidator.validateUpdateContent(reqObj);
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

  public Result getHealth() {
    return Results.ok("ok");
  }

  /**
   * @param all
   * @return
   */
  public Result preflight(String all) {
    return ok().withHeader("Access-Control-Allow-Origin", "*")
        .withHeader("Allow", "*")
        .withHeader("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
        .withHeader(
            "Access-Control-Allow-Headers",
            "Origin, X-Requested-With, Content-Type, Accept, Referer, User-Agent,X-Consumer-ID,cid,ts,X-Device-ID,X-Authenticated-Userid,X-msgid,id,X-Access-TokenId");
  }

  /**
   * This method is called by Admininistrator to update learner current state with last store state.
   *
   * @return Result
   */
  public CompletionStage<Result> updateContentStateByAdmin(Http.Request httpRequest) {
    JsonNode requestData = httpRequest.body().asJson();
    String loggingHeaders =  httpRequest.attrs().getOptional(Attrs.X_LOGGING_HEADERS).orElse(null);
    String requestedBy = httpRequest.attrs().getOptional(Attrs.USER_ID).orElse(null);
    String apiDebugLog = "UpdateContentState Request: " + requestData.toString() + " RequestedBy: " + requestedBy;
    try {
      Request reqObj = (Request) mapper.RequestMapper.mapRequest(requestData, Request.class);
      String requestedFor = (String) reqObj.getRequest().getOrDefault(JsonKey.USER_ID, null);
      RequestValidator.validateUpdateContent(reqObj);
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
   * Updates the state of an event based on the incoming HTTP request.
   * <p>
   * This method processes the JSON payload of the request, validates it,
   * transforms necessary fields, and forwards the request to an actor for
   * further processing. It also logs relevant information and handles
   * exceptions that may occur during the process.
   *
   * @param httpRequest The incoming HTTP request containing the event update data.
   * @return A CompletionStage<Result> representing the outcome of the event update operation.
   */
  public CompletionStage<Result> updateEventState(Http.Request httpRequest) {
    // Extracting the request body as JSON
    JsonNode requestData = httpRequest.body().asJson();
    String loggingHeaders = httpRequest.attrs().getOptional(Attrs.X_LOGGING_HEADERS).orElse(null);
    String requestedBy = httpRequest.attrs().getOptional(Attrs.USER_ID).orElse(null);
    String requestedFor = httpRequest.attrs().getOptional(Attrs.REQUESTED_FOR).orElse(null);
    String apiDebugLog = "UpdateEventState Request: " + requestData.toString() + " RequestedBy: " + requestedBy + " RequestedFor: " + requestedFor + " ";
    try {
      Request reqObj = (Request) mapper.RequestMapper.mapRequest(requestData, Request.class);
      // Validating the request
      RequestValidator.validateUpdateEvent(reqObj);
      // Transforming the user ID for the request
      reqObj = transformUserId(reqObj);
      reqObj.setOperation("updateEventConsumption");
      // Setting additional request attributes
      reqObj.setRequestId(httpRequest.attrs().getOptional(Attrs.REQUEST_ID).orElse(null));
      reqObj.setEnv(getEnvironment());
      HashMap<String, Object> innerMap = new HashMap<>();
      innerMap.put(JsonKey.REQUESTED_BY, requestedBy);
      // Adding the requestedFor attribute if it is not blank
      if (StringUtils.isNotBlank(requestedFor))
        innerMap.put(SunbirdKey.REQUESTED_FOR, requestedFor);
      if (Boolean.FALSE.equals(reqObj.contains(JsonKey.EVENTS))) {
        // If not, populate the innerMap with event and batch IDs
        innerMap.put(JsonKey.EVENT_ID, reqObj.getOrDefault(JsonKey.EVENT_ID, ""));
        innerMap.put(JsonKey.BATCH_ID, reqObj.getOrDefault(JsonKey.BATCH_ID, ""));
      } else {
        // If events are present, add them to the innerMap
        innerMap.put(JsonKey.EVENTS, reqObj.get(JsonKey.EVENTS));
      }
      innerMap.put(JsonKey.USER_ID, reqObj.getRequest().get(JsonKey.USER_ID));
      reqObj.setRequest(innerMap);
      // Sending the request to the actor for processing
      CompletionStage<Result> result = actorResponseHandler(contentConsumptionActor, reqObj, timeout, null, httpRequest);
      return result.thenApplyAsync(r -> {
        logger.info(null, apiDebugLog + ":: ResponseStatus: " + r.status() + " Headers: " + loggingHeaders);
        return r;
      });
    } catch (Exception e) {
      return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest)).thenApplyAsync(r -> {
        logger.info(null, apiDebugLog + ":: ResponseStatus: " + r.status() + " Headers: " + loggingHeaders + " ErrMessage: " + e.getMessage());
        return r;
      });
    }
  }
}
