package org.sunbird.learner.actors.accesssettings;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.accesssettings.dao.impl.AccessSettingsDaoImpl;
import org.sunbird.learner.actors.accesssettings.model.AccessControl;
import org.sunbird.userorg.UserOrgServiceImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared access-settings evaluation logic used by both the /v2/course/enroll eligibility check
 * (CourseEnrollmentRequestValidator) and the Comprehensive-Assessment mandatory-course eligibility
 * check (ExtendedCourseEnrollmentActor), so there is a single source of truth for how a user's
 * profile is turned into attributes and matched against a course's access_setting_rules_v2 rules.
 */
public final class AccessSettingsUtil {

  private AccessSettingsUtil() {
  }

  /**
   * Builds the flat attribute map (user, rootorgid, profilestatus, designation, group, cadre,
   * service, batch, isoncentraldeputation) that UserGroupCriteria.evaluate() matches criteriaKey
   * against.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, String> getUserAttributes(Map<String, Object> userProfileMap) {
    Map<String, String> userAttributes = new HashMap<>();
    userAttributes.put(JsonKey.USER, (String) userProfileMap.get(JsonKey.ID));
    userAttributes.put(JsonKey.ROOT_ORG_ID.toLowerCase(), (String) userProfileMap.get(JsonKey.ROOT_ORG_ID));
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
            userAttributes.put(JsonKey.GROUP, (String) professionalDetails.get(JsonKey.GROUP));
          }
          if (profileDetails.containsKey(JsonKey.CADRE_DETAILS)) {
            Map<String, Object> cadreDetails = (Map<String, Object>) profileDetails.get(JsonKey.CADRE_DETAILS);
            if (MapUtils.isNotEmpty(cadreDetails)) {
              userAttributes.put(JsonKey.CADRE, (String) cadreDetails.get(JsonKey.CADRE_NAME));
              userAttributes.put(JsonKey.SERVICE, (String) cadreDetails.get(JsonKey.CIVIL_SERVICE_NAME));
              if (cadreDetails.containsKey(JsonKey.CADRE_BATCH)) {
                userAttributes.put(JsonKey.BATCH, String.valueOf(cadreDetails.get(JsonKey.CADRE_BATCH)));
              }
              if (cadreDetails.containsKey(JsonKey.IS_ON_CENTRAL_DEPUTATION) && null != cadreDetails.get(JsonKey.IS_ON_CENTRAL_DEPUTATION)) {
                userAttributes.put(JsonKey.IS_ON_CENTRAL_DEPUTATION.toLowerCase(), String.valueOf(cadreDetails.get(JsonKey.IS_ON_CENTRAL_DEPUTATION)));
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

  /**
   * Full per-course access-settings eligibility check for a single user: skips (returns true) if
   * the course has no accessSettingsEnabled flag set; fails closed (returns false) if the flag is
   * set but no access_setting_rules_v2 row exists, or the user profile can't be resolved;
   * otherwise evaluates the course's rules against the user's live profile attributes.
   *
   * @param courseDetails content metadata map for courseId, must contain JsonKey.ACCESS_SETTINGS_ENABLED
   */
  public static boolean isUserEligibleForAccessSettings(
      RequestContext requestContext, Map<String, Object> courseDetails, String courseId, String userId) {
    Boolean accessSettingsEnabled = (Boolean) courseDetails.get(JsonKey.ACCESS_SETTINGS_ENABLED);
    if (accessSettingsEnabled == null || !accessSettingsEnabled) {
      return true;
    }
    AccessControl accessControl = AccessSettingsDaoImpl.getInstance().readAccessSettings(requestContext, courseId);
    if (accessControl == null) {
      return false;
    }
    Map<String, Object> userProfile;
    try {
      userProfile = UserOrgServiceImpl.getInstance().getUserDetailsById(userId, requestContext);
    } catch (Exception e) {
      return false;
    }
    if (MapUtils.isEmpty(userProfile)) {
      return false;
    }
    Map<String, String> userProfileAttributes = getUserAttributes(userProfile);
    return RuleEngineValidator.getInstance().evaluateRules(userProfileAttributes, accessControl.getUserGroups());
  }
}
