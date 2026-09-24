/** */
package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.RequestContext;

import java.util.Map;

/**
 * Calls cb-ext-course-service's Comprehensive-Assessment eligibility API to resolve, for the
 * calling user, whether a given CA do_id is linked (via caLinkedId) to any CbPlan they're
 * eligible for, and what courses are mandatory prerequisites under that plan. The plan search
 * (current + previous financial year, org/ministry scoping, access-control rule evaluation)
 * happens entirely server-side - this class does not parse or search a dictionary response
 * itself.
 */
public final class CbPlanUtil {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final LoggerUtil logger = new LoggerUtil(CbPlanUtil.class);

  private CbPlanUtil() {}

  /**
   * Fetches CA eligibility + mandatory-course identifiers for the calling user and the given
   * do_id. The user is derived server-side by cb-ext-course-service from the forwarded auth
   * token, so {@code headers} must contain x-authenticated-user-token. Throws {@link
   * CbPlanLookupException} if the call fails or the response can't be parsed - callers must not
   * treat this the same as a legitimate "not eligible" outcome (that would misreport a system
   * failure as ineligibility).
   *
   * @return a map with "eligible" (Boolean) and "mandatoryCourses" (List&lt;String&gt;)
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> fetchComprehensiveAssessmentEligibility(
      String doId, Map<String, String> headers, RequestContext requestContext) {
    try {
      String url =
          ProjectUtil.getConfigValue(JsonKey.CB_EXT_COURSE_SERVICE_BASE_URL)
              + JsonKey.CB_PLAN_USER_ELIGIBILITY_URL.replace(
                  JsonKey.CB_PLAN_DO_ID_PLACEHOLDER, doId);

      logger.info(requestContext, "URL :" + url);
      String response = HttpUtil.sendGetRequest(url, headers);
      if (response == null || response.isEmpty()) {
        logger.info(requestContext, "response :" + response);
        logger.error(
            requestContext, "CbPlanUtil: empty response from CA eligibility API", null);
        throw new CbPlanLookupException("Empty response from CA eligibility API", null);
      }
      Map<String, Object> parsed = mapper.readValue(response, Map.class);
      Object result = parsed.get(JsonKey.RESULT);
      logger.info(requestContext, "result :" + result);
      if (!(result instanceof Map)) {
        logger.info(requestContext, "Malformed CA eligibility response :" + result);
        throw new CbPlanLookupException("Malformed CA eligibility response", null);
      }
      return (Map<String, Object>) result;
    } catch (CbPlanLookupException e) {
      throw e;
    } catch (Exception e) {
      logger.info(requestContext, "CbPlanUtil: error fetching CA eligibility :" + e.getMessage());
      logger.error(requestContext, "CbPlanUtil: error fetching CA eligibility", e);
      throw new CbPlanLookupException("Failed to fetch CA eligibility", e);
    }
  }
}
