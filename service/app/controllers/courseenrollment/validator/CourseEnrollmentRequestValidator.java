package controllers.courseenrollment.validator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.logstash.logback.encoder.org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.learner.actors.accesssettings.model.AccessControl;
import org.sunbird.learner.actors.accesssettings.dao.impl.AccessSettingsDaoImpl;
import org.sunbird.learner.actors.accesssettings.model.AccessControl;
import org.sunbird.learner.util.BatchCacheHandler;
import org.sunbird.learner.util.ContentCacheHandlerV2;
import org.sunbird.learner.util.ContentUtil;
import org.sunbird.userorg.UserOrgServiceImpl;

import java.util.*;
import java.util.stream.Collectors;

public class CourseEnrollmentRequestValidator extends BaseRequestValidator {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  private static LoggerUtil logger = new LoggerUtil(ContentUtil.class);

  public CourseEnrollmentRequestValidator() {}

  List<String> acceptedStatus = new ArrayList<>(Arrays.asList("In-Progress", "Completed", "Not-Started", "All"));

  public void validateEnrollCourse(Request courseRequestDto) {
    commonValidations(courseRequestDto);
  }

  public void validateEnrollProgram(Request programRequestDto) {
    commonValidationsProgram(programRequestDto);
  }
  public void validateUnenrollCourse(Request courseRequestDto) {
    commonValidations(courseRequestDto);
  }

  private void commonValidations(Request courseRequestDto) {
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.COURSE_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.COURSE_ID+"/"+JsonKey.COLLECTION_ID);
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.BATCH_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.BATCH_ID);
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.USER_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.USER_ID);
  }

  public void validateEnrolledCourse(Request courseRequestDto) {
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.BATCH_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.BATCH_ID);
    validateParam(
        (String) courseRequestDto.getRequest().get(JsonKey.USER_ID),
        ResponseCode.mandatoryParamsMissing,
        JsonKey.USER_ID);
  }

  public void validateUserEnrolledCourse(Request courseRequestDto) {
    validateParam(
            (String) courseRequestDto.get(JsonKey.USER_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.USER_ID);
  }

  public void validateCourseParticipant(Request courseRequestDto) {
    validateParam(
            (String) courseRequestDto.getRequest().get(JsonKey.COURSE_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.COURSE_ID+"/"+JsonKey.ENROLLABLE_ITEM_ID+"/"+JsonKey.COLLECTION_ID);
    validateParam(
            (String) courseRequestDto.getRequest().get(JsonKey.BATCH_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.BATCH_ID+"/"+JsonKey.FIXED_BATCH_ID);
  }

  private void commonValidationsProgram(Request programRequestDto) {
    validateParam(
            (String) programRequestDto.getRequest().get(JsonKey.PROGRAM_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.PROGRAM_ID+"/"+JsonKey.COLLECTION_ID);
    validateParam(
            (String) programRequestDto.getRequest().get(JsonKey.BATCH_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.BATCH_ID);
    validateParam(
            (String) programRequestDto.getRequest().get(JsonKey.USER_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.USER_ID);
  }

  public void bulkEnrollValidationsForProgram(Request programRequestDto) {
    validateParam(
            (String) programRequestDto.getRequest().get(JsonKey.PROGRAM_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.PROGRAM_ID+"/"+JsonKey.COLLECTION_ID);
    validateParam(
            (String) programRequestDto.getRequest().get(JsonKey.BATCH_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.BATCH_ID);

    List<String> userIdList = (List<String>) programRequestDto.getRequest().get(JsonKey.USERID_LIST);
    validateParamCollection(
            userIdList,ResponseCode.mandatoryParamsMissing,
            JsonKey.USERID_LIST);
    if (userIdList != null&& userIdList.size()<30) {
      for (String userId : userIdList) {
        validateParam(
                userId,ResponseCode.invalidParameterValue,
                JsonKey.USER_ID);
      }
    }
  }

  public void validateEnrollListRequest(Request enrollListRequestDto) {
    validateParam(
            (String) enrollListRequestDto.getRequest().get(JsonKey.STATUS),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.STATUS);
    validateParamFromList(acceptedStatus,
            (String) enrollListRequestDto.getRequest().get(JsonKey.STATUS),
            ResponseCode.invalidData,
            JsonKey.STATUS);
  }

  public void validateEnrollListRequestDetails(Request enrollListRequestDetailsDto) {
    validateParamCollection(
            (List<String>) enrollListRequestDetailsDto.getRequest().get(JsonKey.COURSE_ID),
            ResponseCode.mandatoryParamsMissing,
            JsonKey.COURSE_ID);
  }

  public void validateEnrolmentCriteria(Request requestDto, boolean isCourse, boolean isBlendedProgram) {
    // Get the courseId from the request
    String courseId = "";
    if (isCourse) {
      courseId = (String) requestDto.getRequest().get(JsonKey.COURSE_ID);
      if (StringUtils.isBlank(courseId)) {
        throw new ProjectCommonException(
            ResponseCode.courseIdRequired.getErrorCode(),
            ResponseCode.courseIdRequired.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      courseId = (String) requestDto.getRequest().get(JsonKey.PROGRAM_ID);
      if (StringUtils.isBlank(courseId)) {
        throw new ProjectCommonException(
            ResponseCode.programIdRequired.getErrorCode(),
            ResponseCode.programIdRequired.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
    
    // Get the course details from ContentCahceHandlerV2
    Map<String, Object> courseDetails = null;
    try {
      courseDetails = ContentCacheHandlerV2.getInstance().getContent(courseId);
    } catch (Exception e) {
      throw new ProjectCommonException(
           ResponseCode.courseNotFound.getErrorCode(),
          ResponseCode.courseNotFound.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (ObjectUtils.isEmpty(courseDetails)) {
      throw new ProjectCommonException(
          ResponseCode.courseNotFound.getErrorCode(),
          ResponseCode.courseNotFound.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    Object preEnrolmentResourcesObj = courseDetails.get("preEnrolmentResources");
    if (preEnrolmentResourcesObj instanceof List && !((List<?>) preEnrolmentResourcesObj).isEmpty()) {
      validatePreEnrolmentResources((List<Map<String, Object>>) preEnrolmentResourcesObj, requestDto, courseDetails);
    }

    // Check if the course has accessRules enabled
    Boolean accessSettingsEnabled = (Boolean) courseDetails.get(JsonKey.ACCESS_SETTINGS_ENABLED);
    if (accessSettingsEnabled == null || !accessSettingsEnabled) {
      // If accessSettingsEnabled is not present or false, it means access rules are not enabled for the course
      return;
    }
    // Use batchId for access settings if isBlendedProgram, else use courseId
    String accessSettingsId = courseId;
    if (isBlendedProgram) {
      accessSettingsId = (String) requestDto.getRequest().get(JsonKey.BATCH_ID);
      if (StringUtils.isBlank(accessSettingsId)) {
        throw new ProjectCommonException(
                ResponseCode.batchIdRequired.getErrorCode(),
                ResponseCode.batchIdRequired.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      courseId = accessSettingsId;
      courseDetails = BatchCacheHandler.getBatch(courseId);
    }
    // Parse the accessRules from the course details
    AccessControl accessControl = AccessSettingsDaoImpl.getInstance().readAccessSettings(requestDto.getRequestContext(), courseId);
    if (accessControl == null) {
      String errorMsg = isCourse
        ? ResponseMessage.Message.ACCESS_RULES_ENABLED_BUT_NOT_FOUND_COURSE
        : ResponseMessage.Message.ACCESS_RULES_ENABLED_BUT_NOT_FOUND_PROGRAM;
      throw new ProjectCommonException(
          ResponseCode.accessRulesEnabledButNotFound.getErrorCode(),
          errorMsg,
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    // Get the userId from the request
    String userId = (String) requestDto.getRequest().get(JsonKey.USER_ID);
    Map<String, Object> userProfile = null;
    Map<String, String> userProfileAttributes = null;
    try {
      // Fetch user profile details using UserOrgServiceImpl
      userProfile = (Map<String, Object>) UserOrgServiceImpl.getInstance().getUserDetailsById(userId, null);
      userProfileAttributes = getUserAttributes(userProfile);
    } catch (Exception e) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (ObjectUtils.isEmpty(userProfile)) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    
    // If accessRules are enabled, check if the user has access to the course
    Boolean isCourseAllowed = RuleEngineValidator.getInstance().evaluateRules(userProfileAttributes, accessControl.getUserGroups());
    if (!isCourseAllowed) {
      throw new ProjectCommonException(
          ResponseCode.userNotEligibleForEnrollment.getErrorCode(),
          ResponseCode.userNotEligibleForEnrollment.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private Map<String, String> getUserAttributes(Map<String, Object> userProfileMap) {
    Map<String, String> userAttributes = new HashMap<>();
    userAttributes.put(JsonKey.USER, (String) userProfileMap.get(JsonKey.ID));
    userAttributes.put(JsonKey.ROOT_ORG_ID.toLowerCase(), (String) userProfileMap.get(JsonKey.ROOT_ORG_ID));
    String designation = null, userGroup = null, profileStatus = null;
    String profileDetailsStr = (String) userProfileMap.get(JsonKey.PROFILE_DETAILS);
    try {
      if (StringUtils.isNotBlank(profileDetailsStr)) {
        Map<String, Object> profileDetails = new ObjectMapper().readValue(profileDetailsStr, new TypeReference<Map<String, Object>>() {
        });
        if (MapUtils.isNotEmpty(profileDetails)) {
          userAttributes.put(JsonKey.PROFILE_STATUS.toLowerCase(), (String) profileDetails.get(JsonKey.PROFILE_STATUS));
    
          Map<String, Object> professionalDetails = (profileDetails.containsKey(JsonKey.PROFESSIONAL_DETAILS)) ? 
              ((List<Map<String, Object>>) profileDetails.get(JsonKey.PROFESSIONAL_DETAILS)).get(0) : null;
          if (MapUtils.isNotEmpty(professionalDetails)) {
            userAttributes.put(JsonKey.DESIGNATION, (String) professionalDetails.get(JsonKey.DESIGNATION));
            userAttributes.put(JsonKey.GROUP,  (String) professionalDetails.get(JsonKey.GROUP));
          }
          if (profileDetails.containsKey(JsonKey.CADRE_DETAILS)) {
            Map<String, Object> cadreDetails = (Map<String, Object>) profileDetails.get(JsonKey.CADRE_DETAILS);
            if (MapUtils.isNotEmpty(cadreDetails)) {
              userAttributes.put(JsonKey.CADRE, (String) cadreDetails.get(JsonKey.CADRE_NAME));
              userAttributes.put(JsonKey.SERVICE, (String) cadreDetails.get(JsonKey.CIVIL_SERVICE_NAME));
              if (cadreDetails.containsKey(JsonKey.CADRE_BATCH)) {
                userAttributes.put(JsonKey.BATCH, String.valueOf(cadreDetails.get(JsonKey.CADRE_BATCH)));
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    return userAttributes;
  }

  public void validatePreEnrolmentResources(List<Map<String, Object>> preEnrolmentResources, Request req, Map<String, Object> courseDetails) {
    if (CollectionUtils.isEmpty(preEnrolmentResources)) {
      return;
    }
    boolean hasMandatory = false;
    String userId = (String) req.getRequest().get(JsonKey.USER_ID);
    for (Map<String, Object> resource : preEnrolmentResources) {
      Boolean isMandatory = (Boolean) resource.get("isMendatory");
      if (Boolean.TRUE.equals(isMandatory)) {
        hasMandatory = true;
        String resourceId = (String) resource.get("identifier");
        // Replace with actual Cassandra check for user completion
        boolean isCompleted = checkResourceCompletionInCassandra(userId, resourceId, resource);
        if (!isCompleted) {
          throw new ProjectCommonException(
                  ResponseCode.preEnrolmentResourceNotCompleted.getErrorCode(),
                  "Mandatory pre-enrolment resource not completed: " + resource.get("name"),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
    if (!hasMandatory) {
      return;
    }
  }

  public void validatePreEnrolmentResources(List<Map<String, Object>> preEnrolmentResources, Request req) {
    if (CollectionUtils.isEmpty(preEnrolmentResources)) {
      return;
    }
    boolean hasMandatory = false;
    String userId = (String) req.getRequest().get(JsonKey.USER_ID);
    for (Map<String, Object> resource : preEnrolmentResources) {
      Boolean isMandatory = (Boolean) resource.get("isMendatory");
      if (Boolean.TRUE.equals(isMandatory)) {
        hasMandatory = true;
        String resourceId = (String) resource.get("identifier");
        // Replace with actual Cassandra check for user completion
        boolean isCompleted = checkResourceCompletionInCassandra(userId, resourceId, resource);
        if (!isCompleted) {
          throw new ProjectCommonException(
                  ResponseCode.preEnrolmentResourceNotCompleted.getErrorCode(),
                  "Mandatory pre-enrolment resource not completed: " + resource.get("name"),
                  ResponseCode.CLIENT_ERROR.getResponseCode());
        }
      }
    }
    if (!hasMandatory) {
      return;
    }
  }

  private boolean checkResourceCompletionInCassandra(String userId, String resourceId, Map<String, Object> resource) {
    String contentType = (String) resource.get("contentType");
    RequestContext requestContext = new RequestContext("default", "default", "default", "default", "default", "default", "default", null);
    List<String> fields = new ArrayList<>();
    fields.add("status");

    if ("SelfAssess".equals(contentType)) {
      Map<String, Object> assessmentPropertyMap = new HashMap<>();
      assessmentPropertyMap.put(JsonKey.USER_ID_KEY, userId);
      assessmentPropertyMap.put(JsonKey.RESOURCE_ID, resourceId);

      Response assessmentDetails = cassandraOperation.getRecords(
              requestContext,
              JsonKey.KEYSPACE_SUNBIRD,
              "user_assessment_data",
              assessmentPropertyMap,
              fields
      );

      if (assessmentDetails != null && assessmentDetails.getResult() != null) {
        Object resultObj = assessmentDetails.getResult().get(JsonKey.RESPONSE);
        if (resultObj instanceof List && !((List<?>) resultObj).isEmpty()) {
          Object statusObj = ((Map<?, ?>) ((List<?>) resultObj).get(0)).get("status");
          if ("SUBMITTED".equalsIgnoreCase(String.valueOf(statusObj))) {
            return true;
          } else {
            throw new ProjectCommonException(
                    ResponseCode.preEnrolmentResourceNotCompleted.getErrorCode(),
                    "Mandatory self-assessment not submitted: " + resource.get("name"),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
          }
        }
      }
      // If no record found, treat as not completed
      throw new ProjectCommonException(
              ResponseCode.preEnrolmentResourceNotCompleted.getErrorCode(),
              "Mandatory self-assessment not submitted: " + resource.get("name"),
              ResponseCode.CLIENT_ERROR.getResponseCode());
    } else {
      Map<String, Object> propertyMap = new HashMap<>();
      propertyMap.put(JsonKey.USER_ID_KEY, userId);
      propertyMap.put(JsonKey.RESOURCE_ID, resourceId);

      Response userContentDetails = cassandraOperation.getRecords(
              requestContext,
              JsonKey.KEYSPACE_SUNBIRD_RESOURCE,
              JsonKey.USER_ENTITY_CONSUMPTION,
              propertyMap,
              fields
      );

      if (userContentDetails != null && userContentDetails.getResult() != null) {
        Object resultObj = userContentDetails.getResult().get(JsonKey.RESPONSE);
        if (resultObj instanceof List && !((List<?>) resultObj).isEmpty()) {
          Object statusObj = ((Map<?, ?>) ((List<?>) resultObj).get(0)).get("status");
          if (statusObj instanceof Integer) {
            return ((Integer) statusObj) == 2;
          } else if (statusObj != null) {
            try {
              return Integer.parseInt(statusObj.toString()) == 2;
            } catch (NumberFormatException e) {
              // Ignore and return false
            }
          }
        }
      }
      return false;
    }
  }

  public Map<String, String> validateLanguageSupport(String reqLang, String courseId) {
    Map<String, String> result = new HashMap<>();
    List<String> fields = Arrays.asList(JsonKey.LANGUAGE, JsonKey.LANGUAGE_MAP, JsonKey.COURSECATEGORY, JsonKey.IDENTIFIER);
    Map<String, Object> contentResponse = ContentUtil.getContent(courseId, fields);
    Map<String, Object> contentData = (Map<String, Object>) contentResponse.get(JsonKey.CONTENT);
    if (contentData == null || contentData.isEmpty()) {
      throw new ProjectCommonException(
              ResponseCode.resourceNotFound.getErrorCode(),
              "Content 'content' node is missing or empty for courseId: " + courseId,
              ResponseCode.RESOURCE_NOT_FOUND.getResponseCode()
      );
    }
    String courseCategory = (String) contentData.getOrDefault(JsonKey.COURSECATEGORY, "");
    String recentLangFromMultilingual = null;
    if (JsonKey.MULTILINGUAL_COURSE.equalsIgnoreCase(courseCategory)) {
      //if courseCategory is multilingual course then we are replacing with base language courseId.
      Map<String, String> courseIdWithLanguage = getBaseLanguageId(contentData);
      courseId = courseIdWithLanguage.get(JsonKey.ID);
      recentLangFromMultilingual = courseIdWithLanguage.get(JsonKey.RECENT_LANGUAGE);
      contentResponse = ContentUtil.getContent(courseId, fields);
      contentData = (Map<String, Object>) contentResponse.get(JsonKey.CONTENT);
    }
    result.put(JsonKey.COURSE_ID, courseId);

    List<String> baseLangList = new ArrayList<>();
    Object baseLangObj = contentData.get(JsonKey.LANGUAGE);
    if (baseLangObj instanceof List) {
      baseLangList = ((List<?>) baseLangObj)
              .stream()
              .map(Object::toString)
              .map(String::toLowerCase)
              .collect(Collectors.toList());
    }
    String baseLang = baseLangList.isEmpty() ? null : baseLangList.get(0);
    if (StringUtils.isNotBlank(reqLang)) {
      //If reqLang is provided, validate it
      if (baseLangList.contains(reqLang.toLowerCase())) {
        result.put(JsonKey.RECENT_LANGUAGE, reqLang.toLowerCase());
        return result;
      }
      Map<String, Map<String, Object>> languageMap = new HashMap<>();
      Object langMapObj = contentData.get(JsonKey.LANGUAGE_MAP);
      if (langMapObj instanceof Map) {
        Map<?, ?> tempMap = (Map<?, ?>) langMapObj;
        for (Map.Entry<?, ?> entry : tempMap.entrySet()) {
          if (entry.getValue() instanceof Map) {
            languageMap.put(entry.getKey().toString().toLowerCase(),
                    (Map<String, Object>) entry.getValue());
          }
        }
      }
      if (!languageMap.containsKey(reqLang.toLowerCase())) {
        throw new ProjectCommonException(
                ResponseCode.invalidParameterValue.getErrorCode(),
                String.format(JsonKey.LANGUAGE_NOT_IN_BASE_OR_MAP, reqLang),
                ResponseCode.CLIENT_ERROR.getResponseCode()
        );
      }
      String status = String.valueOf(languageMap.get(reqLang.toLowerCase()).getOrDefault("status", ""));
      if (!JsonKey.LIVE.equalsIgnoreCase(status)) {
        throw new ProjectCommonException(
                ResponseCode.invalidParameterValue.getErrorCode(),
                String.format(JsonKey.LANGUAGE_NOT_LIVE, reqLang, status),
                ResponseCode.CLIENT_ERROR.getResponseCode()
        );
      }
      result.put(JsonKey.RECENT_LANGUAGE, reqLang.toLowerCase());
      return result;
    } else {
      if (StringUtils.isNotBlank(recentLangFromMultilingual)) {
        result.put(JsonKey.RECENT_LANGUAGE, recentLangFromMultilingual.toLowerCase());
      } else if (StringUtils.isNotBlank(baseLang)) {
        result.put(JsonKey.RECENT_LANGUAGE, baseLang);
      } else {
        throw new ProjectCommonException(
                ResponseCode.mandatoryParamsMissing.getErrorCode(),
                String.format(JsonKey.LANGUAGE_AND_BASE_MISSING, courseId),
                ResponseCode.CLIENT_ERROR.getResponseCode()
        );
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> getBaseLanguageId(Map<String, Object> contentData) {
    Object langMapV1Obj = contentData.get(JsonKey.LANGUAGE_MAP);

    if (langMapV1Obj instanceof Map) {
      Map<String, Object> langMapV1 = (Map<String, Object>) langMapV1Obj;

      for (Map.Entry<String, Object> entry : langMapV1.entrySet()) {
        String language = entry.getKey();
        Object langEntry = entry.getValue();

        if (langEntry instanceof Map) {
          Map<String, Object> langDetails = (Map<String, Object>) langEntry;

          boolean isBaseLanguage = Boolean.parseBoolean(String.valueOf(langDetails.getOrDefault(JsonKey.IS_BASE_LANGUAGE, false)));
          String status = String.valueOf(langDetails.getOrDefault(JsonKey.STATUS, ""));

          if (isBaseLanguage && JsonKey.LIVE.equalsIgnoreCase(status)) {
            String baseContentId = String.valueOf(langDetails.get(JsonKey.ID));
            if (StringUtils.isNotBlank(baseContentId)) {
              logger.info(null, JsonKey.MULTILINGUAL_COURSE_SWITCH_LOG+ baseContentId);
              Map<String, String> result = new HashMap<>();
              result.put(JsonKey.ID, baseContentId);
              result.put(JsonKey.RECENT_LANGUAGE, language);
              return result;
            }
          }
        }
      }
    }
    throw new ProjectCommonException(
            ResponseCode.invalidParameterValue.getErrorCode(),
            JsonKey.ERROR_MULTILINGUAL_BASE_LANG_NOT_FOUND,
            ResponseCode.CLIENT_ERROR.getResponseCode()
    );
  }
}
