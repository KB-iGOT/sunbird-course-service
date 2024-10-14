package controllers.eventmanagement;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.courseenrollment.validator.CourseEnrollmentRequestValidator;
import controllers.coursemanagement.validator.CourseBatchRequestValidator;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class EventsController extends BaseController {

    @Inject
    @Named("event-batch-management-actor")
    private ActorRef eventsActor;

    private CourseEnrollmentRequestValidator validator = new CourseEnrollmentRequestValidator();

    public CompletionStage<Result> createEventBatch(Http.Request httpRequest) {
        return handleRequest(
                eventsActor,
                ActorOperations.CREATE_EVENT_BATCH.getValue(),
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    String eventId = req.getRequest().containsKey(JsonKey.EVENT_ID) ? JsonKey.EVENT_ID : JsonKey.COLLECTION_ID;
                    req.getRequest().put(JsonKey.EVENT_ID, req.getRequest().get(eventId));
                    new CourseBatchRequestValidator().validateCreateEventBatchRequest(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    public CompletionStage<Result> enrollEvent(Http.Request httpRequest) {
        return handleRequest(eventsActor, "enrollEvent",
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    Map<String, String[]> queryParams = new HashMap<>(httpRequest.queryString());
                    String courseId = req.getRequest().containsKey(JsonKey.EVENT_ID) ? JsonKey.EVENT_ID : JsonKey.COLLECTION_ID;
                    String batchId = (String)req.getRequest().get(JsonKey.BATCH_ID);
                    req.getRequest().put(JsonKey.EVENT_ID, req.getRequest().get(courseId));
                    String userId = (String) req.getContext().getOrDefault(JsonKey.REQUESTED_FOR, req.getContext().get(JsonKey.REQUESTED_BY));
                    validator.validateRequestedBy(userId);
                    logger.info( ((Request) request).getRequestContext(), " EventsController : Request for enroll received, UserId : "+  userId +", courseId : "+courseId + ", batchId:"+batchId);
                    req.getRequest().put(JsonKey.USER_ID, userId);
                    new CourseBatchRequestValidator().validateEventEnroll(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }
}
