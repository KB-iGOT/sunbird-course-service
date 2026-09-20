/** */
package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.RequestContext;

/**
 * Calls cb-ext-course-service's CbPlan user-dictionary API to resolve, for the calling user,
 * which CbPlan (if any) links to a given Comprehensive Assessment do_id, and what courses are
 * mandatory prerequisites under that plan.
 *
 * Response shape (confirmed against a real sample):
 * result.&lt;planYear&gt;.aparPlanList and .nonAparPlanList are MAPS keyed by planId (not lists),
 * each plan carrying contentList: [{identifier, mandatory}] and comprehensiveAssessment: do_id|null.
 */
public final class CbPlanUtil {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final LoggerUtil logger = new LoggerUtil(CbPlanUtil.class);

  private CbPlanUtil() {}

  /**
   * Fetches the calling user's CbPlan dictionary. The user is derived server-side by
   * cb-ext-course-service from the forwarded auth token, so {@code headers} must contain
   * x-authenticated-user-token. Returns an empty map on any failure (fail-closed).
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> getCbPlanDictionary(
      Map<String, String> headers, RequestContext requestContext) {
    try {
      String url =
          ProjectUtil.getConfigValue(JsonKey.CB_EXT_COURSE_SERVICE_BASE_URL)
              + JsonKey.CB_PLAN_USER_DICTIONARY_URL;
      String response = HttpUtil.sendPostRequest(url, "{\"request\":{}}", headers);
      if (response == null || response.isEmpty()) {
        logger.error(requestContext, "CbPlanUtil: empty response from CbPlan dictionary API", null);
        return new HashMap<>();
      }
      return mapper.readValue(response, Map.class);
    } catch (Exception e) {
      logger.error(requestContext, "CbPlanUtil: error fetching CbPlan dictionary", e);
      return new HashMap<>();
    }
  }

  /**
   * Scans every plan year's aparPlanList/nonAparPlanList (each a Map keyed by planId) for the
   * plan whose comprehensiveAssessment identifier equals doId. Returns null if none found (or on
   * a malformed response) - fail-closed, meaning "not eligible".
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> findPlanForComprehensiveAssessment(
      Map<String, Object> dictionaryResponse, String doId) {
    try {
      Map<String, Object> result = (Map<String, Object>) dictionaryResponse.get(JsonKey.RESULT);
      if (result == null) {
        return null;
      }
      for (Object yearEntryObj : result.values()) {
        Map<String, Object> yearEntry = (Map<String, Object>) yearEntryObj;
        Map<String, Object> match = findInPlanMap(yearEntry.get("aparPlanList"), doId);
        if (match != null) {
          return match;
        }
        match = findInPlanMap(yearEntry.get("nonAparPlanList"), doId);
        if (match != null) {
          return match;
        }
      }
    } catch (Exception e) {
      logger.error(null, "CbPlanUtil: error parsing CbPlan dictionary response", e);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> findInPlanMap(Object planMapObj, String doId) {
    if (!(planMapObj instanceof Map)) {
      return null;
    }
    Map<String, Object> planMap = (Map<String, Object>) planMapObj;
    for (Object planObj : planMap.values()) {
      if (planObj instanceof Map) {
        Map<String, Object> plan = (Map<String, Object>) planObj;
        if (doId.equals(plan.get("comprehensiveAssessment"))) {
          return plan;
        }
      }
    }
    return null;
  }

  /** Extracts the identifiers marked mandatory:true in a plan's contentList. */
  @SuppressWarnings("unchecked")
  public static List<String> getMandatoryCourseIds(Map<String, Object> plan) {
    Object contentListObj = plan.get("contentList");
    List<String> mandatoryIds = new ArrayList<>();
    if (contentListObj instanceof List) {
      for (Object entryObj : (List<Object>) contentListObj) {
        if (entryObj instanceof Map) {
          Map<String, Object> entry = (Map<String, Object>) entryObj;
          if (Boolean.TRUE.equals(entry.get("mandatory")) && entry.get("identifier") != null) {
            mandatoryIds.add((String) entry.get("identifier"));
          }
        }
      }
    }
    return mandatoryIds;
  }
}
