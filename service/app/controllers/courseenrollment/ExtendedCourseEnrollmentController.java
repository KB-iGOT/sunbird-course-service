package controllers.courseenrollment;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.courseenrollment.validator.CourseEnrollmentRequestValidator;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class ExtendedCourseEnrollmentController extends BaseController {

    @Inject
    @Named("extended-course-enrolment-actor")
    private ActorRef courseEnrolmentActor;

    private CourseEnrollmentRequestValidator validator = new CourseEnrollmentRequestValidator();

    public CompletionStage<Result> enrollCourseWithLanguage(Http.Request httpRequest) {
        return handleRequest(courseEnrolmentActor, "enrollV2",
                httpRequest.body().asJson(),
                (requestObj) -> {
                    Request req = (Request) requestObj;
                    Map<String, Object> requestMap = req.getRequest();

                    // Extract courseId from COURSE_ID or COLLECTION_ID
                    String courseIdKey = requestMap.containsKey(JsonKey.COURSE_ID) ? JsonKey.COURSE_ID : JsonKey.COLLECTION_ID;
                    String courseId = (String) requestMap.get(courseIdKey);
                    requestMap.put(JsonKey.COURSE_ID, courseId);

                    // Extract batchId
                    String batchId = (String) requestMap.get(JsonKey.BATCH_ID);
                    String userId = (String) req.getContext().getOrDefault(JsonKey.REQUESTED_FOR, req.getContext().get(JsonKey.REQUESTED_BY));
                    requestMap.put(JsonKey.USER_ID, userId);
                    // Normalize language if present
                    String reqLang = null;
                    if (requestMap.containsKey(JsonKey.LANGUAGE)) {
                        reqLang = ((String) requestMap.get(JsonKey.LANGUAGE)).toLowerCase();
                        requestMap.put(JsonKey.LANGUAGE, reqLang);
                    }
                    logger.info(req.getRequestContext(),
                            "CourseEnrollmentController : enrollCourseWithLanguage request received, userId=" + userId +
                                    ", courseId=" + courseId + ", batchId=" + batchId);

                    // Validations
                    validator.validateRequestedBy(userId);
                    validator.validateEnrollCourse(req);
                    validator.validateEnrolmentCriteria(req, true, false);
                    validator.validateLanguageSupport(reqLang, courseId);

                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }
}
