package controllers.courseenrollment.validator;

import net.logstash.logback.encoder.org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.learner.actors.accesssettings.model.AccessControl;
import org.sunbird.learner.actors.accesssettings.dao.impl.AccessSettingsDaoImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.sunbird.learner.util.ContentCacheHandlerV2;
import org.sunbird.userorg.UserOrgServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.collections4.MapUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

public class CourseEnrollmentRequestValidator extends BaseRequestValidator {

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

  public void validateEnrolmentCriteria(Request requestDto, boolean isCourse) {
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
    // Check if the course has accessRules enabled
    Boolean accessSettingsEnabled = (Boolean) courseDetails.get(JsonKey.ACCESS_SETTINGS_ENABLED);
    if (accessSettingsEnabled == null || !accessSettingsEnabled) {
      // If accessSettingsEnabled is not present or false, it means access rules are not enabled for the course
      return;
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
}
