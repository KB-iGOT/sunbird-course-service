package controllers.eventmanagement;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.coursemanagement.validator.CourseBatchRequestValidator;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CompletionStage;

public class EventsController extends BaseController {

    @Inject
    @Named("event-batch-management-actor")
    private ActorRef eventsActorRef;

    public CompletionStage<Result> createEventBatch(Http.Request httpRequest) {
        return handleRequest(
                eventsActorRef,
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
}
