package util;

import org.apache.commons.collections.CollectionUtils;
import org.sunbird.common.Constants;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.ContentCacheHandlerV2;

import java.util.List;
import java.util.Map;

import static org.sunbird.common.request.orgvalidator.BaseOrgRequestValidator.ERROR_CODE;

public class ExtendedLearnerStateRequestValidator extends BaseRequestValidator {

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
        String contentId = (contentIds != null && CollectionUtils.isNotEmpty(contentIds))
                ? contentIds.get(0)
                : null;

        Map<String, Object> courseContent;
        if (contentId != null) {
            courseContent = fetchCourseContent(contentId);
        } else {
            String courseId = (String) request.getRequest().get(JsonKey.COURSE_ID);
            if (org.apache.commons.lang3.StringUtils.isBlank(courseId)) {
                courseId = (String) request.getRequest().get(JsonKey.COLLECTION_ID);
            }
            courseContent = fetchCourseContent(courseId);
        }

        if (courseContent == null) {
            throw new ProjectCommonException(
                    ResponseCode.invalidCourseId.getErrorCode(),
                    "Course content not found for the given id.",
                    ERROR_CODE
            );
        }

        List<String> languages = (List<String>) courseContent.get(JsonKey.LANGUAGE);
        String language = (String) request.getRequest().get(JsonKey.LANGUAGE);

        if (org.apache.commons.lang3.StringUtils.isBlank(language)) {
            if (CollectionUtils.isNotEmpty(languages)) {
                request.getRequest().put(JsonKey.LANGUAGE, languages.get(0).toLowerCase());
            } else {
                throw new ProjectCommonException(
                        ResponseCode.languageRequired.getErrorCode(),
                        ResponseCode.languageRequired.getErrorMessage(),
                        ERROR_CODE
                );
            }
        } else {
            boolean match = CollectionUtils.isNotEmpty(languages) &&
                    languages.stream()
                            .map(String::toLowerCase)
                            .anyMatch(lang -> lang.equals(language.toLowerCase()));
            if (!match) {
                throw new ProjectCommonException(
                        ResponseCode.languageRequired.getErrorCode(),
                        ResponseCode.languageRequired.getErrorMessage(),
                        ERROR_CODE
                );
            }
            request.getRequest().put(JsonKey.LANGUAGE, language.toLowerCase());
        }
    }

    public Map<String, Object> fetchCourseContent(String contentId) {
        try {
            return ContentCacheHandlerV2.getInstance().getContent(contentId);
        } catch (Exception e) {
            logger.error(null, "Error fetching course content for contentId: " + contentId, e);
            return null;
        }
    }
}
