package controllers.eventmanagement;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.courseenrollment.validator.CourseEnrollmentRequestValidator;
import controllers.eventmanagement.validator.EventRequestValidator;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class EventController extends BaseController {

    @Inject
    @Named("event-management-actor")
    private ActorRef actorRef;

    private CourseEnrollmentRequestValidator validator = new CourseEnrollmentRequestValidator();

    public CompletionStage<Result> discard(String id, Http.Request httpRequest) {
        ProjectLogger.log(
                "Discard event method is called = " + httpRequest.body().asJson(),
                LoggerEnum.DEBUG.name());
        return handleRequest(
                actorRef,
                ActorOperations.DELETE_EVENT.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    ((Request) request).getRequest().put("identifier", id);
                    EventRequestValidator.validateFixedBatchId((Request) request);
                    return null;
                },
                httpRequest);
    }

    public CompletionStage<Result> getEnrolledCourses(String uid, Http.Request httpRequest) {
        return handleRequest(actorRef, "listEnrol",
                httpRequest.body().asJson(),
                (req) -> {
                    Request request = (Request) req;
                    Map<String, String[]> queryParams = new HashMap<>(httpRequest.queryString());
                    if(queryParams.containsKey("fields")) {
                        Set<String> fields = new HashSet<>(Arrays.asList(queryParams.get("fields")[0].split(",")));
                        fields.addAll(Arrays.asList(JsonKey.NAME, JsonKey.DESCRIPTION, JsonKey.LEAF_NODE_COUNT, JsonKey.APP_ICON));
                        queryParams.put("fields", fields.toArray(new String[0]));
                    }
                    if(queryParams.containsKey("courseIds")) {
                        List<String> courseIds = new ArrayList<>(Arrays.asList(queryParams.get("courseIds")[0].split(",")));
                        request.put("courseIds",courseIds );
                    }
                    String userId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
                    validator.validateRequestedBy(userId);
                    request.getContext().put(JsonKey.USER_ID, userId);
                    request.getRequest().put(JsonKey.USER_ID, userId);

                    request
                            .getContext()
                            .put(JsonKey.URL_QUERY_STRING, getQueryString(queryParams));
                    request
                            .getContext()
                            .put(JsonKey.BATCH_DETAILS, httpRequest.queryString().get(JsonKey.BATCH_DETAILS));
                    if (queryParams.containsKey("cache")) {
                        request.getContext().put("cache", Boolean.parseBoolean(queryParams.get("cache")[0]));
                    } else
                        request.getContext().put("cache", true);
                    return null;
                },
                null,
                null,
                getAllRequestHeaders((httpRequest)),
                false,
                httpRequest);
    }

} 
