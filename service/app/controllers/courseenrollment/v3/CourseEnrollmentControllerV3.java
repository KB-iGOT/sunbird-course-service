package controllers.courseenrollment.v3;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.courseenrollment.validator.CourseEnrollmentRequestValidator;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;
import java.util.concurrent.CompletionStage;


public class CourseEnrollmentControllerV3 extends BaseController {

    @Inject
    @Named("course-enrolment-actor-v3")
    private ActorRef courseEnrolmentActorV3;

    private CourseEnrollmentRequestValidator validator = new CourseEnrollmentRequestValidator();

    public CompletionStage<Result> getEnrolledCoursesV3(String uid, Http.Request httpRequest) {
        return handleRequest(courseEnrolmentActorV3, "list",
                httpRequest.body().asJson(),
                (req) -> {
                    Request request = (Request) req;
                    String userId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
                    validator.validateRequestedBy(userId);
                    request.getContext().put(JsonKey.USER_ID, userId);
                    request.getRequest().put(JsonKey.USER_ID, userId);
                    validator.validateEnrollListRequest(request);
                    return null;
                },
                getAllRequestHeaders((httpRequest)),
                httpRequest);
    }

    public CompletionStage<Result> enrolmentUserInfoStats(String uid, Http.Request httpRequest) {
        return handleRequest(courseEnrolmentActorV3, "enrolmentInfoStats",
                httpRequest.body().asJson(),
                (req) -> {
                    Request request = (Request) req;
                    String userId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
                    validator.validateRequestedBy(userId);
                    request.getContext().put(JsonKey.USER_ID, userId);
                    request.getRequest().put(JsonKey.USER_ID, userId);
                    return null;
                },
                null,
                null,
                getAllRequestHeaders((httpRequest)),
                false,
                httpRequest);
    }

    public CompletionStage<Result> getEnrolledCoursesDetailsV3(String uid, Http.Request httpRequest) {
        return handleRequest(courseEnrolmentActorV3, "enrolV3Details",
                httpRequest.body().asJson(),
                (req) -> {
                    Request request = (Request) req;
                    String userId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
                    validator.validateRequestedBy(userId);
                    request.getContext().put(JsonKey.USER_ID, userId);
                    request.getRequest().put(JsonKey.USER_ID, userId);
                    validator.validateEnrollListRequestDetails(request);
                    return null;
                },
                getAllRequestHeaders((httpRequest)),
                httpRequest);
    }
}
