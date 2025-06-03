package controllers.courseenrollment.validator;

import net.logstash.logback.encoder.org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.BaseRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
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

  public void validateEnrolmentCriteria(Request enrolmentCriteriaRequestDto) {
    // Get the courseId from the request
    String courseId = (String) enrolmentCriteriaRequestDto.getRequest().get(JsonKey.COURSE_ID);
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
    AccessControl accessControl = AccessSettingsDaoImpl.getInstance().readAccessSettings(enrolmentCriteriaRequestDto.getRequestContext(), courseId);
    if (accessControl == null) {
      throw new ProjectCommonException(
          ResponseCode.accessRulesEnabledButNotFound.getErrorCode(),
          ResponseCode.accessRulesEnabledButNotFound.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    // Get the userId from the request
    String userId = (String) enrolmentCriteriaRequestDto.getRequest().get(JsonKey.USER_ID);
    Map<String, Object> userProfile = null;
    
    try {
      // Fetch user profile details using UserOrgServiceImpl
      userProfile = (Map<String, Object>) UserOrgServiceImpl.getInstance().getUserDetailsById(userId, null);
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
    String errMsg = RuleEngineValidator.getInstance().evaluateRules(getUserAttributes(userProfile), accessControl.getUserGroups());
    if (StringUtils.isNotBlank(errMsg)) {
      throw new ProjectCommonException(
          ResponseCode.userNotEligibleForEnrollment.getErrorCode(),
          ResponseCode.userNotEligibleForEnrollment.getErrorMessage() + " " + errMsg,
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  private Map<String, String> getUserAttributes(Map<String, Object> userProfileMap) {
    Map<String, String> userAttributes = new HashMap<>();
    userAttributes.put(JsonKey.USER_ID, (String) userProfileMap.get(JsonKey.ID));
    userAttributes.put(JsonKey.ROOT_ORG_ID, (String) userProfileMap.get(JsonKey.ROOT_ORG_ID));
    String designation = null, userGroup = null, profileStatus = null;
    Map<String, Object> profileDetails = (Map<String, Object>) userProfileMap.get(JsonKey.PROFILE_DETAILS);
    if (MapUtils.isNotEmpty(profileDetails)) {
      profileStatus = (String) profileDetails.get(JsonKey.PROFILE_STATUS);

      Map<String, Object> professionalDetails = (profileDetails.containsKey(JsonKey.PROFESSIONAL_DETAILS)) ? 
          ((List<Map<String, Object>>) profileDetails.get(JsonKey.PROFESSIONAL_DETAILS)).get(0) : null;
      if (MapUtils.isNotEmpty(professionalDetails)) {
        designation = (String) professionalDetails.get(JsonKey.DESIGNATION);
        userGroup = (String) professionalDetails.get(JsonKey.GROUP);
      }
    }
    userAttributes.put(JsonKey.DESIGNATION, designation);
    userAttributes.put(JsonKey.GROUP, userGroup);
    userAttributes.put(JsonKey.PROFILE_STATUS, profileStatus);
    return userAttributes;
  }
}
