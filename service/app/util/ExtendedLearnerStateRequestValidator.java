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
        String courseId = (String) request.getRequest().get(JsonKey.COURSE_ID);
        List<String> contentIds = (List<String>) request.getRequest().get(JsonKey.CONTENT_IDS);
        String incomingLang = (String) request.getRequest().get(JsonKey.LANGUAGE);

        Map<String, Object> enrolmentData = fetchEnrolmentData(request);
        String recentLang = (String) enrolmentData.get(JsonKey.RECENT_LANGUAGE);

        Map<String, Object> courseContent = fetchCourseContent(courseId);
        Map<String, Object> languageMapV1 = (Map<String, Object>) courseContent.get(JsonKey.LANGUAGE_MAP);

        if (StringUtils.isNotBlank(incomingLang)) {
            if (incomingLang.toLowerCase().equalsIgnoreCase(recentLang.toLowerCase()) && CollectionUtils.isEmpty(contentIds)) {
                //String multiCourseId = languageMapV1.get(incomingLang.toLowerCase());
                Map<String, Object> langEntry = (Map<String, Object>) languageMapV1.get(incomingLang.toLowerCase());
                String multiCourseId = (String) langEntry.get(JsonKey.ID);
                List<String> leafNodes = fetchLeafNodes(multiCourseId);
                request.getRequest().put(JsonKey.CONTENT_IDS, leafNodes);
            } else if (CollectionUtils.isEmpty(contentIds)) {
                Map<String, Object> langEntry = (Map<String, Object>) languageMapV1.get(incomingLang.toLowerCase());
                String multiCourseId = (String) langEntry.get(JsonKey.ID);
                List<String> leafNodes = fetchLeafNodes(multiCourseId);
                request.getRequest().put(JsonKey.CONTENT_IDS, leafNodes);
            }
            request.getRequest().put(JsonKey.LANGUAGE, incomingLang.toLowerCase());
            return;
        }

        if (StringUtils.isBlank(incomingLang)) {
            if (StringUtils.isNotBlank(recentLang)) {
                Map<String, Object> langEntry = (Map<String, Object>) languageMapV1.get(recentLang.toLowerCase());
                String multiCourseId = (String) langEntry.get(JsonKey.ID);
                List<String> leafNodes = fetchLeafNodes(multiCourseId);
                request.getRequest().put(JsonKey.LANGUAGE, recentLang.toLowerCase());
                request.getRequest().put(JsonKey.CONTENT_IDS, leafNodes);
                return;
            } else {
                throw new ProjectCommonException(
                        ResponseCode.languageRequired.getErrorCode(),
                        "Language is not provided and no recent_language found.",
                        ERROR_CODE
                );
            }
        }
    }

    private List<String> fetchLeafNodes(String courseId) {
        Map<String, Object> courseData = fetchCourseContent(courseId);
        return (List<String>) courseData.getOrDefault(JsonKey.LEAF_NODES, Collections.emptyList());
    }

    public Map<String, Object> fetchCourseContent(String contentId) {
        try {
            return ContentCacheHandlerV2.getInstance().getContent(contentId);
        } catch (Exception e) {
            logger.error(null, "Error fetching course content for contentId: " + contentId, e);
            return null;
        }
    }

    private Map<String, Object> fetchEnrolmentData(Request request) {
        String userId = (String) request.getRequest().get(JsonKey.USER_ID);
        String courseId = (String) request.getRequest().get(JsonKey.COURSE_ID);
        String batchId = (String) request.getRequest().get(JsonKey.BATCH_ID);

        Map<String, Object> filters = new HashMap<>();
        filters.put(JsonKey.USER_ID_KEY, userId);
        filters.put(JsonKey.COURSE_ID_KEY, courseId);
        filters.put(JsonKey.BATCH_ID_KEY, batchId);

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
                    String.format(
                            "Enrolment not found for user: %s, course: %s, batch: %s",
                            userId, courseId, batchId
                    ),
                    ERROR_CODE
            );
        }

        Map<String, Object> enrolmentData = resultList.get(0);

        String language = (String) request.getRequest().get(JsonKey.LANGUAGE);
        if (StringUtils.isBlank(language)) {
            language = (String) enrolmentData.get(JsonKey.RECENT_LANGUAGE);
            request.getRequest().put(JsonKey.LANGUAGE, language);
        }

        return enrolmentData;
    }

}
