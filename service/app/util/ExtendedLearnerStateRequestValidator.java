package util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Constants;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ContentCacheHandlerV2;
import org.sunbird.learner.util.ExtendedUtil;

import java.util.*;

import static org.sunbird.common.request.orgvalidator.BaseOrgRequestValidator.ERROR_CODE;

public class ExtendedLearnerStateRequestValidator extends BaseRequestValidator {

    private CassandraOperation cassandraOperation =  ServiceFactory.getInstance();
    private static final ExtendedUtil.DbInfo enrolmentDBInfo = ExtendedUtil.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    /**
     * Method to validate the get content state request.
     *
     * @param request Representing the request object.
     */
    public void validateGetContentState(Request request) {
        validateListParam(request.getRequest(), JsonKey.COURSE_IDS, JsonKey.CONTENT_IDS);
        if (request.getRequest().containsKey(JsonKey.COURSE_IDS)) {
            List courseIds = (List) request.getRequest().get(JsonKey.COURSE_IDS);
            request.getRequest().remove(JsonKey.COURSE_IDS);
            if (!request.getRequest().containsKey(JsonKey.COURSE_ID) && !request.getRequest().containsKey(JsonKey.COLLECTION_ID) && CollectionUtils.isNotEmpty(courseIds)) {
                request.getRequest().put(JsonKey.COURSE_ID, courseIds.get(0));
            }
        }
        String courseId = request.getRequest().containsKey(JsonKey.COURSE_ID) ? JsonKey.COURSE_ID : JsonKey.COLLECTION_ID;
        request.getRequest().put(JsonKey.COURSE_ID, request.getRequest().get(courseId));
        validateAndSetLanguage(request);
        checkMandatoryFieldsPresent(request.getRequest(), JsonKey.USER_ID, JsonKey.COURSE_ID, JsonKey.BATCH_ID);
    }

    public void validateAndSetLanguage(Request request) {
        List<String> contentIds = (List<String>) request.getRequest().get(JsonKey.CONTENT_IDS);
        String language = (String) request.getRequest().get(JsonKey.LANGUAGE);

        if (CollectionUtils.isNotEmpty(contentIds)) {
            String contentId = contentIds.get(0);
            Map<String, Object> courseContent = fetchCourseContent(contentId);

            if (courseContent == null) {
                throw new ProjectCommonException(
                        ResponseCode.invalidCourseId.getErrorCode(),
                        "Course content not found for contentId: " + contentId,
                        ERROR_CODE
                );
            }

            if (StringUtils.isBlank(language)) {
                List<String> contentLanguages = (List<String>) courseContent.get(JsonKey.LANGUAGE);
                if (CollectionUtils.isEmpty(contentLanguages)) {
                    throw new ProjectCommonException(
                            ResponseCode.languageRequired.getErrorCode(),
                            "Language not provided and could not be inferred from content metadata.",
                            ERROR_CODE
                    );
                }
                language = contentLanguages.get(0).toLowerCase();
                request.getRequest().put(JsonKey.LANGUAGE, language);
            }
            return;
        }

        setContentIdsFromEnrolment(request);
    }

    public Map<String, Object> fetchCourseContent(String contentId) {
        try {
            return ContentCacheHandlerV2.getInstance().getContent(contentId);
        } catch (Exception e) {
            logger.error(null, "Error fetching course content for contentId: " + contentId, e);
            return null;
        }
    }

    public void setContentIdsFromEnrolment(Request request) {
        List<String> contentIdsReq = (List<String>) request.getRequest().get(JsonKey.CONTENT_IDS);
        if (CollectionUtils.isNotEmpty(contentIdsReq)) {
            return;
        }
        String userId = (String) request.getRequest().get(JsonKey.USER_ID);
        String courseId = (String) request.getRequest().get(JsonKey.COURSE_ID);
        String batchId = (String) request.getRequest().get(JsonKey.BATCH_ID);

        Map<String, Object> filters = new HashMap<>();
        filters.put("userid", userId);
        filters.put("courseid", courseId);
        filters.put("batchid", batchId);

        Response response = cassandraOperation.getRecords(
                request.getRequestContext(),
                enrolmentDBInfo.getKeySpace(),
                enrolmentDBInfo.getTableName(),
                filters,
                null
        );

        List<Map<String, Object>> resultList = (List<Map<String, Object>>)
                response.getResult().getOrDefault(JsonKey.RESPONSE, new ArrayList<>());

        if (resultList.isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "Enrolment not found for user: " + userId + ", course: " + courseId + ", batch: " + batchId,
                    ERROR_CODE
            );
        }

        Map<String, Object> enrolmentData = resultList.get(0);

        String language = (String) request.getRequest().get(JsonKey.LANGUAGE);
        if (StringUtils.isBlank(language)) {
            language = (String) enrolmentData.get("recent_language");
        }

        if (StringUtils.isBlank(language)) {
            throw new ProjectCommonException(
                    ResponseCode.languageRequired.getErrorCode(),
                    "Language is not provided and no recent_language found in enrolment.",
                    ERROR_CODE
            );
        }

        Map<String, Object> langContentStatusMap = (Map<String, Object>) enrolmentData.get("langContentStatus");
        if (langContentStatusMap == null || !langContentStatusMap.containsKey(language)) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "No content mapping found for language: " + language,
                    ERROR_CODE
            );
        }

        Map<String, Object> contentStatusMap = (Map<String, Object>) langContentStatusMap.get(language);
        if (contentStatusMap == null || contentStatusMap.isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    "No content IDs found for language: " + language,
                    ERROR_CODE
            );
        }

        List<String> contentIds = new ArrayList<>(contentStatusMap.keySet());
        request.getRequest().put(JsonKey.LANGUAGE, language.toLowerCase());
        request.getRequest().put(JsonKey.CONTENT_IDS, contentIds);
    }
}
