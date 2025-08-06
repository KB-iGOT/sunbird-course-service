package controllers.courseenrollment;

import akka.actor.ActorRef;
import controllers.BaseController;
import controllers.courseenrollment.validator.CourseEnrollmentRequestValidator;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class ExtendedCourseEnrollmentController extends BaseController {

    @Inject
    @Named("extended-course-enrolment-actor")
    private ActorRef extendedCourseEnrolmentActor;

    private CourseEnrollmentRequestValidator validator = new CourseEnrollmentRequestValidator();

    public CompletionStage<Result> enrollCourseWithLanguage(Http.Request httpRequest) {
        return handleRequest(extendedCourseEnrolmentActor, "enrollV2",
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
                        reqLang = ((String) requestMap.get(JsonKey.LANGUAGE));
                        requestMap.put(JsonKey.LANGUAGE, reqLang);
                    }
                    logger.info(req.getRequestContext(),
                            "extendedCourseEnrolmentActor : enrollCourseWithLanguage request received, userId=" + userId +
                                    ", courseId=" + courseId + ", batchId=" + batchId + ",RequestLanguage" + reqLang);

                    // Validations
                    validator.validateRequestedBy(userId);
                    validator.validateEnrollCourse(req);
                    validator.validateEnrolmentCriteria(req, true, false);
                    Map<String, String> validatedLangAndContent = validator.validateLanguageSupport(reqLang, courseId);
                    if (MapUtils.isNotEmpty(validatedLangAndContent)
                            && StringUtils.isNotBlank(MapUtils.getString(validatedLangAndContent, JsonKey.COURSE_ID))) {
                        requestMap.put(JsonKey.RECENT_LANGUAGE, validatedLangAndContent.get(JsonKey.RECENT_LANGUAGE));
                        requestMap.put(JsonKey.COURSE_ID, validatedLangAndContent.get(JsonKey.COURSE_ID));
                    }
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    public CompletionStage<Result> getEnrolledCoursesDetails(String uid, Http.Request httpRequest) {
        return handleRequest(extendedCourseEnrolmentActor, "enrolV3Details",
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

    public CompletionStage<Result> getEnrolledCourses(String uid, Http.Request httpRequest) {
        return handleRequest(extendedCourseEnrolmentActor, "list",
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

    public CompletionStage<Result> privateGetEnrolledCoursesV3(String uid, Http.Request httpRequest) {
        return handleRequest(extendedCourseEnrolmentActor, "privateList",
                httpRequest.body().asJson(),
                (req) -> {
                    Request request = (Request) req;
                    request.getRequest().put(JsonKey.USER_ID, uid);
                    return null;
                },
                getAllRequestHeaders((httpRequest)),
                httpRequest);
    }

    public CompletionStage<Result> enrolmentUserInfoStats(String uid, Http.Request httpRequest) {
        return handleRequest(extendedCourseEnrolmentActor, "enrolmentInfoStats",
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

    public CompletionStage<Result> enrollProgramV2(Http.Request httpRequest) {
        return enrollProgramV2(httpRequest, false);
    }

    public CompletionStage<Result> enrollProgramV2(Http.Request httpRequest, Boolean batchType) {
        return handleRequest(extendedCourseEnrolmentActor, "enrolProgramV2",
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    Map<String, String[]> queryParams = new HashMap<>(httpRequest.queryString());
                    String programId = req.getRequest().containsKey(JsonKey.PROGRAM_ID) ? JsonKey.PROGRAM_ID : JsonKey.COLLECTION_ID;
                    req.getRequest().put(JsonKey.PROGRAM_ID, req.getRequest().get(programId));
                    String userId = (String) req.getContext().getOrDefault(JsonKey.REQUESTED_FOR, req.getContext().get(JsonKey.REQUESTED_BY));
                    req.getRequest().put(JsonKey.IS_ADMIN_API, false);
                    validator.validateRequestedBy(userId);
                    validator.validateEnrollProgram(req);
                    validator.validateEnrolmentCriteria(req, false, false);
                    req.getRequest().put(JsonKey.USER_ID, userId);
                    req.getContext().put("verifyBatchType", batchType);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    public CompletionStage<Result> blendedProgramEnrollCourseV2(Http.Request httpRequest) {
        return handleRequest(extendedCourseEnrolmentActor, "enrolBlendedProgramV2",
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    Map<String, String[]> queryParams = new HashMap<>(httpRequest.queryString());
                    String courseId = req.getRequest().containsKey(JsonKey.COURSE_ID) ? JsonKey.COURSE_ID : JsonKey.COLLECTION_ID;
                    String userId = (String) req.getRequest().get(JsonKey.USER_ID);
                    String batchId = (String) req.getRequest().get(JsonKey.BATCH_ID);
                    req.getRequest().put(JsonKey.COURSE_ID, req.getRequest().get(courseId));
                    logger.info( ((Request) request).getRequestContext(), " CourseEnrollmentController : Request for enroll recieved via Blended Program admin enroll, UserId : "+  userId +", courseId : "+courseId+ ", batchId:"+batchId);
                    validator.validateEnrollCourse(req);
                    //call validateEnrollmentCriteriaMethod validateEnrolmentCriteria
                    validator.validateEnrolmentCriteria(req, false, false);

                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    public CompletionStage<Result> adminBulkEnrollProgramV3(Http.Request httpRequest) {
        return handleRequest(extendedCourseEnrolmentActor, "bulkEnrolProgramV3",
                httpRequest.body().asJson(),
                (request) -> {
                    Request req = (Request) request;
                    Map<String, String[]> queryParams = new HashMap<>(httpRequest.queryString());
                    String programId = req.getRequest().containsKey(JsonKey.PROGRAM_ID) ? JsonKey.PROGRAM_ID : JsonKey.COLLECTION_ID;
                    req.getRequest().put(JsonKey.PROGRAM_ID, req.getRequest().get(programId));
                    req.getRequest().put(JsonKey.IS_ADMIN_API, true);
                    validator.bulkEnrollValidationsForProgram(req);
                    return null;
                },
                getAllRequestHeaders(httpRequest),
                httpRequest);
    }

    public CompletionStage<Result> getEnrolledCoursesDetailsWithProgress(String uid, Http.Request httpRequest) {
        return handleRequest(extendedCourseEnrolmentActor, "enrolDetailsWithProgress",
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
