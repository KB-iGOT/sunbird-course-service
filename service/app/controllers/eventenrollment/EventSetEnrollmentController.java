package controllers.eventenrollment;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.courseenrollment.validator.CourseEnrollmentRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CompletionStage;

public class EventSetEnrollmentController extends BaseController {

    @Inject
    @Named("eventset-enrolment-actor")
    private ActorRef actorRef;

    public CompletionStage<Result> enroll(Http.Request httpRequest) {
        return handleRequest(actorRef, "enrol",
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    Util.handleFixedBatchIdRequest(req);
                    new CourseEnrollmentRequestValidator().validateEnrollCourse(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    public CompletionStage<Result> unenroll(Http.Request httpRequest) {
        return handleRequest(actorRef, "unenrol",
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    Util.handleFixedBatchIdRequest(req);
                    new CourseEnrollmentRequestValidator().validateUnenrollCourse(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

}