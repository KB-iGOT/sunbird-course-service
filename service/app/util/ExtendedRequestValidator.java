package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Constants;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.ContentCacheHandlerV2;
import org.sunbird.learner.util.ExtendedUtil;

import java.util.*;

import static org.sunbird.common.request.orgvalidator.BaseOrgRequestValidator.ERROR_CODE;

public class ExtendedRequestValidator {

    private static final int ERROR_CODE = ResponseCode.CLIENT_ERROR.getResponseCode();
    public static LoggerUtil logger = new LoggerUtil(RequestValidator.class);
    private static ObjectMapper mapper = new ObjectMapper();
    private static CassandraOperation cassandraOperation =  ServiceFactory.getInstance();
    private static final ExtendedUtil.DbInfo enrolmentDBInfo = ExtendedUtil.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);


    /**
     * This method will do content state request data validation. if all mandatory data is coming then
     * it won't do any thing if any mandatory data is missing then it will throw exception.
     *
     * @param contentRequestDto Request
     */
    @SuppressWarnings("unchecked")
    public static void validateUpdateContent(Request contentRequestDto, boolean isAdminRequest) throws Exception {
        List<Map<String, Object>> list =
                (List<Map<String, Object>>) (contentRequestDto.getRequest().get(JsonKey.CONTENTS));
        if(CollectionUtils.isNotEmpty(list)) {
            for (Map<String, Object> map : list) {
                if (null != map.get(JsonKey.LAST_UPDATED_TIME)) {
                    boolean bool =
                            ProjectUtil.isDateValidFormat(
                                    "yyyy-MM-dd HH:mm:ss:SSSZ", (String) map.get(JsonKey.LAST_UPDATED_TIME));
                    if (!bool) {
                        throw new ProjectCommonException(
                                ResponseCode.dateFormatError.getErrorCode(),
                                ResponseCode.dateFormatError.getErrorMessage(),
                                ERROR_CODE);
                    }
                }
                if (null != map.get(JsonKey.LAST_COMPLETED_TIME)) {
                    boolean bool =
                            ProjectUtil.isDateValidFormat(
                                    "yyyy-MM-dd HH:mm:ss:SSSZ", (String) map.get(JsonKey.LAST_COMPLETED_TIME));
                    if (!bool) {
                        throw new ProjectCommonException(
                                ResponseCode.dateFormatError.getErrorCode(),
                                ResponseCode.dateFormatError.getErrorMessage(),
                                ERROR_CODE);
                    }
                }
                String contentId = "";
                if (map.containsKey(JsonKey.CONTENT_ID)) {
                    if (null == map.get(JsonKey.CONTENT_ID)) {
                        throw new ProjectCommonException(
                                ResponseCode.contentIdRequired.getErrorCode(),
                                ResponseCode.contentIdRequiredError.getErrorMessage(),
                                ERROR_CODE);
                    }
                    contentId = (String) map.get(JsonKey.CONTENT_ID);
                    if (ProjectUtil.isNull(map.get(JsonKey.STATUS))) {
                        throw new ProjectCommonException(
                                ResponseCode.contentStatusRequired.getErrorCode(),
                                ResponseCode.contentStatusRequired.getErrorMessage(),
                                ERROR_CODE);
                    }
                } else {
                    throw new ProjectCommonException(
                            ResponseCode.contentIdRequired.getErrorCode(),
                            ResponseCode.contentIdRequiredError.getErrorMessage(),
                            ERROR_CODE);
                }

                map.put(JsonKey.COURSE_ID, map.containsKey(JsonKey.COURSE_ID)
                        ? map.get(JsonKey.COURSE_ID)
                        : map.get(JsonKey.COLLECTION_ID));

                if (StringUtils.isBlank((String) map.get(JsonKey.COURSE_ID))) {
                    throw new ProjectCommonException(
                            ResponseCode.courseIdRequired.getErrorCode(),
                            ResponseCode.courseIdRequiredError.getErrorMessage(),
                            ERROR_CODE);
                }
                Map<String, Object> courseDetails = getCourseContent((String) map.get(JsonKey.COURSE_ID));
                if (isProgramConsumptionAccepted((String) courseDetails.get(JsonKey.COURSECATEGORY), contentId,  (Boolean) courseDetails.get("cumulativeTracking"))) {
                    throw new ProjectCommonException(
                            ResponseCode.invalidProgramId.getErrorCode(),
                            ResponseCode.invalidProgramId.getErrorMessage(),
                            ERROR_CODE);
                }
                if (JsonKey.COMPREHENSIVE_ASSESSMENT_PROGRAM.equalsIgnoreCase((String) courseDetails.get(JsonKey.COURSECATEGORY)) && !isAdminRequest) {
                    // this is CAP and non-admin user called. Let's return error by default.
                    throw new ProjectCommonException(
                            ResponseCode.invalidContentId.getErrorCode(),
                            ResponseCode.invalidContentId.getErrorMessage(),
                            ERROR_CODE);
                }
                if (StringUtils.equalsIgnoreCase(Constants.MULTI_LINGUAL_COURSE, (String) courseDetails.get(JsonKey.COURSECATEGORY))) {
                    throw new ProjectCommonException(
                            ResponseCode.languageRequired.getErrorCode(),
                            JsonKey.MULTILINGUAL_COURSE_PROGRESS_UPDATE_ERROR,
                            ERROR_CODE);
                }

                Map<String, Object> languageMapV1 = (Map<String, Object>) courseDetails.get(JsonKey.LANGUAGE_MAP);

                String incomingLanguage = (String) map.get(JsonKey.LANGUAGE);
                Map<String, Object> enrolmentData = fetchEnrolmentData(
                        (String) contentRequestDto.getRequest().get(JsonKey.USER_ID),
                        (String) map.get(JsonKey.COURSE_ID),
                        (String) map.get(JsonKey.BATCH_ID),
                        contentRequestDto
                );
                String recentLanguage = (String) enrolmentData.get(JsonKey.RECENT_LANGUAGE);

                if (StringUtils.isBlank(incomingLanguage)) {
                    if (StringUtils.isBlank(recentLanguage)) {
                        throw new ProjectCommonException(
                                ResponseCode.languageRequired.getErrorCode(),
                                ResponseCode.languageRequired.getErrorMessage(),
                                ERROR_CODE
                        );
                    } else {
                        incomingLanguage = recentLanguage.toLowerCase();
                    }
                } else {
                    incomingLanguage = incomingLanguage.toLowerCase();
                    if (MapUtils.isNotEmpty(languageMapV1)) {
                        if (!languageMapV1.containsKey(incomingLanguage)) {
                            throw new ProjectCommonException(
                                    ResponseCode.languageRequired.getErrorCode(),
                                    ResponseCode.languageRequired.getErrorMessage(),
                                    ERROR_CODE
                            );
                        }
                    }
                }
                map.put(JsonKey.LANGUAGE, incomingLanguage);
            }
        }
        List<Map<String, Object>> assessmentData =
                (List<Map<String, Object>>) contentRequestDto.getRequest().get(JsonKey.ASSESSMENT_EVENTS);
        if (CollectionUtils.isNotEmpty(assessmentData)) {
            for (Map<String, Object> map : assessmentData) {
                if (!map.containsKey(JsonKey.ASSESSMENT_TS)) {
                    throw new ProjectCommonException(
                            ResponseCode.assessmentAttemptDateRequired.getErrorCode(),
                            ResponseCode.assessmentAttemptDateRequired.getErrorMessage(),
                            ERROR_CODE);
                }

                if (!map.containsKey(JsonKey.COURSE_ID)
                        || StringUtils.isBlank((String) map.get(JsonKey.COURSE_ID))) {
                    throw new ProjectCommonException(
                            ResponseCode.courseIdRequired.getErrorCode(),
                            ResponseCode.courseIdRequiredError.getErrorMessage(),
                            ERROR_CODE);
                }

                if (!map.containsKey(JsonKey.CONTENT_ID)
                        || StringUtils.isBlank((String) map.get(JsonKey.CONTENT_ID))) {
                    throw new ProjectCommonException(
                            ResponseCode.contentIdRequired.getErrorCode(),
                            ResponseCode.contentIdRequiredError.getErrorMessage(),
                            ERROR_CODE);
                }

                if (!map.containsKey(JsonKey.BATCH_ID)
                        || StringUtils.isBlank((String) map.get(JsonKey.BATCH_ID))) {
                    throw new ProjectCommonException(
                            ResponseCode.courseBatchIdRequired.getErrorCode(),
                            ResponseCode.courseBatchIdRequired.getErrorMessage(),
                            ERROR_CODE);
                }

                if (!map.containsKey(JsonKey.USER_ID)
                        || StringUtils.isBlank((String) map.get(JsonKey.USER_ID))) {
                    throw new ProjectCommonException(
                            ResponseCode.userIdRequired.getErrorCode(),
                            ResponseCode.userIdRequired.getErrorMessage(),
                            ERROR_CODE);
                }

                if (!map.containsKey(JsonKey.ATTEMPT_ID)
                        || StringUtils.isBlank((String) map.get(JsonKey.ATTEMPT_ID))) {
                    throw new ProjectCommonException(
                            ResponseCode.attemptIdRequired.getErrorCode(),
                            ResponseCode.attemptIdRequired.getErrorMessage(),
                            ERROR_CODE);
                }

                if (!map.containsKey(JsonKey.EVENTS)) {
                    throw new ProjectCommonException(
                            ResponseCode.eventsRequired.getErrorCode(),
                            ResponseCode.eventsRequired.getErrorMessage(),
                            ERROR_CODE);
                }
            }
        }
        // Validation for enrolment sync
        if(CollectionUtils.isEmpty(list) && CollectionUtils.isEmpty(assessmentData)) {
            contentRequestDto.getRequest().put(JsonKey.COURSE_ID, contentRequestDto.getOrDefault(JsonKey.COURSE_ID, contentRequestDto.getOrDefault(JsonKey.COLLECTION_ID, "")));
            if (StringUtils.isBlank((String) contentRequestDto.getOrDefault(JsonKey.COURSE_ID, ""))) {
                throw new ProjectCommonException(
                        ResponseCode.courseIdRequired.getErrorCode(),
                        ResponseCode.courseIdRequiredError.getErrorMessage(),
                        ERROR_CODE);
            }
            if (StringUtils.isBlank((String) contentRequestDto.getOrDefault(JsonKey.BATCH_ID, ""))) {
                throw new ProjectCommonException(
                        ResponseCode.courseBatchIdRequired.getErrorCode(),
                        ResponseCode.courseBatchIdRequired.getErrorMessage(),
                        ERROR_CODE);
            }

            if (StringUtils.isBlank((String) contentRequestDto.getOrDefault(JsonKey.USER_ID, ""))) {
                throw new ProjectCommonException(
                        ResponseCode.userIdRequired.getErrorCode(),
                        ResponseCode.userIdRequired.getErrorMessage(),
                        ERROR_CODE);
            }
        }
    }

    public static Boolean isProgramConsumptionAccepted(String courseCategory, String contentId, Boolean cumulativeTracking) {
        Boolean isProgram = false;
        try {
            if (StringUtils.isBlank(courseCategory)) {
                throw new ProjectCommonException(
                        ResponseCode.invalidCourseCategory.getErrorCode(),
                        ResponseCode.invalidCourseCategory.getErrorMessage(),
                        ERROR_CODE);
            }
            if (isProgramCategory(courseCategory)) {
                if (cumulativeTracking == null) {
                    throw new ProjectCommonException(
                            ResponseCode.invalidTrackingAttribute.getErrorCode(),
                            ResponseCode.invalidTrackingAttribute.getErrorMessage(),
                            ERROR_CODE);
                } else if (cumulativeTracking) {
                    Map<String, Object> resourceContent =  getCourseContent(contentId);
                    String contextCategory = (String) resourceContent.get(JsonKey.CONTEXT_CATEGORY);
                    String primaryCategory = (String) resourceContent.get(JsonKey.PRIMARYCATEGORY);
                    boolean allowed = false;
                    if (StringUtils.isNotBlank(contextCategory)) {
                        allowed = isCategoryAllowed(contextCategory);
                    } else if (StringUtils.isNotBlank(primaryCategory)) {
                        if (JsonKey.BLENDED_PROGRAM.equalsIgnoreCase(courseCategory)
                                && isPrimaryCategoryAllowedForBlendedProgram(primaryCategory)) {
                            allowed = true;
                        } else {
                            allowed = isPrimaryCategoryAllowed(primaryCategory);
                        }
                    }
                    if (allowed) {
                        isProgram = false;
                    } else {
                        isProgram = true;
                    }
                    logger.info(null,
                            "ContextCategory is details Id: " + contentId +
                                    ", contextCategory=" + contextCategory +
                                    ", primaryCategory=" + primaryCategory +
                                    ", isProgram=" + isProgram);
                }
            }
        } catch (Exception e) {
            logger.error(null, "Error during content read parse for Content ID: " + contentId, e);
        }
        return isProgram;
    }

    public static Map<String, Object> getCourseContent(String courseId) throws Exception {
        return ContentCacheHandlerV2.getInstance().getContent(courseId);
    }


    private static boolean isCategoryAllowed(String category) {
        String categoriesList = ProjectUtil.getConfigValue(JsonKey.ALLOWED_RESOURCES_FOR_PROGRAM_STATUS_UPDATE);
        Set<String> allowedCategoryList = new HashSet<>(Arrays.asList(categoriesList.split(",\\s*")));
        return allowedCategoryList.contains(category);
    }

    private static boolean isProgramCategory(String category) {
        String categoriesList = ProjectUtil.getConfigValue(JsonKey.PROGRAM_CATEGORIES);
        Set<String> programCategories = new HashSet<>(Arrays.asList(categoriesList.split(",\\s*")));
        return programCategories.contains(category);
    }

    private static Map<String, Object> fetchEnrolmentData(String userId, String courseId, String batchId, Request request) {

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

    private static boolean isPrimaryCategoryAllowed(String category) {
        String categoriesList = ProjectUtil.getConfigValue(JsonKey.ALLOWED_PRIMARY_CATEGORIES);
        Set<String> allowed = new HashSet<>(Arrays.asList(categoriesList.split(",\\s*")));
        return allowed.contains(category);
    }

    private static boolean isPrimaryCategoryAllowedForBlendedProgram(String category) {
        String categoriesList = ProjectUtil.getConfigValue(JsonKey.BLENDED_PROGRAM_ALLOWED_PRIMARY_CATEGORIES);
        Set<String> allowed = new HashSet<>(Arrays.asList(categoriesList.split(",\\s*")));
        return allowed.contains(category);
    }
}
