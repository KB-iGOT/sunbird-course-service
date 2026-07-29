package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.constants.CourseJsonKey;
import org.sunbird.models.course.batch.CourseBatch;
import org.sunbird.models.event.batch.EventBatch;
import scala.concurrent.Future;

import org.apache.commons.collections4.CollectionUtils;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.learner.actors.course.dao.ContentHierarchyDao;
import org.sunbird.learner.actors.course.dao.impl.ContentHierarchyDaoImpl;
import org.sunbird.learner.actors.coursebatch.dao.CourseBatchDao;
import org.sunbird.learner.actors.coursebatch.dao.impl.CourseBatchDaoImpl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.sunbird.common.exception.ProjectCommonException.throwClientErrorException;
import static org.sunbird.common.exception.ProjectCommonException.throwServerErrorException;
import static org.sunbird.common.models.util.JsonKey.BEARER;
import static org.sunbird.common.models.util.JsonKey.SUNBIRD_AUTHORIZATION;
import static org.sunbird.common.models.util.ProjectUtil.getConfigValue;
import static org.sunbird.common.responsecode.ResponseCode.errorProcessingRequest;

public class CourseBatchUtil {
  private static ElasticSearchService esUtil = EsClientFactory.getInstance(JsonKey.REST);
  private static ObjectMapper mapper = new ObjectMapper();
  private static LoggerUtil logger = new LoggerUtil(CourseBatchUtil.class);
  private static final List<String> changeInDateFormat = JsonKey.CHANGE_IN_DATE_FORMAT;
  private static final List<String> changeInSimpleDateFormat = JsonKey.CHANGE_IN_SIMPLE_DATE_FORMAT;
  private static final List<String> changeInDateFormatAll = JsonKey.CHANGE_IN_DATE_FORMAT_ALL;
  private static final List<String> setEndOfDay = JsonKey.SET_END_OF_DAY;

  private static CourseBatchDao courseBatchDao = new CourseBatchDaoImpl();
  private static ContentHierarchyDao contentHierarchyDao = new ContentHierarchyDaoImpl();
  private static RedisCacheUtil cacheUtil = new RedisCacheUtil();
  private static final Pattern HOUR_REGEX = Pattern.compile("(\\d+)\\s*(?:hr|hour|h)s?", Pattern.CASE_INSENSITIVE);
  private static final Pattern MINUTE_REGEX = Pattern.compile("(\\d+)\\s*(?:min|minute|m)s?", Pattern.CASE_INSENSITIVE);

  private CourseBatchUtil() {}

  public static void syncCourseBatchForeground(RequestContext requestContext, String uniqueId, Map<String, Object> req) {
    logger.info(requestContext, "CourseBatchManagementActor: syncCourseBatchForeground called for course batch ID = "
            + uniqueId);
    req.put(JsonKey.ID, uniqueId);
    req.put(JsonKey.IDENTIFIER, uniqueId);
    Future<String> esResponseF =
        esUtil.save(requestContext, ProjectUtil.EsType.courseBatch.getTypeName(), uniqueId, req);
    String esResponse = (String) ElasticSearchHelper.getResponseFromFuture(esResponseF);
    logger.info(requestContext, "CourseBatchManagementActor::syncCourseBatchForeground: Sync response for course batch ID = "
            + uniqueId
            + " received response = "
            + esResponse);
  }

  public static Map<String, Object> validateCourseBatch(RequestContext requestContext, String courseId, String batchId) {
    Future<Map<String, Object>> resultF =
        esUtil.getDataByIdentifier(requestContext, EsType.courseBatch.getTypeName(), batchId);
    Map<String, Object> result =
        (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(resultF);
    if (MapUtils.isEmpty(result)) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.CLIENT_ERROR, "No such batchId exists");
    }
    if (StringUtils.isNotBlank(courseId)
        && !StringUtils.equals(courseId, (String) result.get(JsonKey.COURSE_ID))) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.CLIENT_ERROR, "batchId is not linked with courseId");
    }
    return result;
  }

  public static Map<String, Object> validateTemplate(RequestContext requestContext, String templateId) {
    Response templateResponse = getTemplate(requestContext, templateId);
    if (templateResponse == null
        || MapUtils.isEmpty(templateResponse.getResult())
        || !(templateResponse.getResult().containsKey(JsonKey.CONTENT) || templateResponse.getResult().containsKey("certificate"))) {
      ProjectCommonException.throwClientErrorException(
          ResponseCode.CLIENT_ERROR, "Invalid template Id: " + templateId);
    }
    Map<String, Object> template =
            templateResponse.getResult().containsKey(JsonKey.CONTENT) ?
                    (Map<String, Object>) templateResponse.getResult().getOrDefault(JsonKey.CONTENT, new HashMap<>()) :
                    (Map<String, Object>) ((Map<String, Object>) templateResponse.getResult().getOrDefault("certificate", new HashMap<>())).getOrDefault(JsonKey.TEMPLATE, new HashMap<>());

    if (MapUtils.isEmpty(template) || !templateId.equals(template.get(JsonKey.IDENTIFIER))) {
      ProjectCommonException.throwClientErrorException(
              ResponseCode.CLIENT_ERROR, "Invalid template Id: " + templateId);
    }
    return template;
  }

  private static Response getTemplate(RequestContext requestContext, String templateId) {
    Response response = null;
    String responseBody = null;
    try {
      responseBody = readTemplate(requestContext, templateId);
      response = mapper.readValue(responseBody, Response.class);
      if (!ResponseCode.OK.equals(response.getResponseCode())) {
        throw new ProjectCommonException(
            response.getResponseCode().name(),
            response.getParams().getErrmsg(),
            response.getResponseCode().getResponseCode());
      }
    } catch (ProjectCommonException e) {
      logger.error(requestContext, 
          "CourseBatchUtil:getResponse ProjectCommonException:"
              + "Request , Status : "
              + e.getCode()
              + " "
              + e.getMessage()
              + ",Response Body :"
              + responseBody, e);
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(requestContext, 
          "CourseBatchUtil:getResponse occurred with error message = "
              + e.getMessage()
              + ", Response Body : "
              + responseBody,
          e);
      throwServerErrorException(
          ResponseCode.SERVER_ERROR, "Exception while validating template with cert service");
    }
    return response;
  }

  private static Map<String, String> getdefaultHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put(AUTHORIZATION, BEARER + getConfigValue(SUNBIRD_AUTHORIZATION));
    headers.put("Content-Type", "application/json");
    return headers;
  }

  private static String readTemplate(RequestContext requestContext, String templateId) throws Exception {
    String templateRelativeUrl = ProjectUtil.getConfigValue("sunbird_cert_template_url");
    String certTemplateReadUrl = ProjectUtil.getConfigValue("sunbird_cert_template_read_url");
    String contentServiceBaseUrl = ProjectUtil.getConfigValue("ekstep_api_base_url");
    String certServiceBaseUrl = ProjectUtil.getConfigValue("sunbird_cert_service_base_url");
    HttpResponse<String> httpResponse = null;
    httpResponse = templateReadResponse(requestContext, contentServiceBaseUrl, templateRelativeUrl, templateId);

    if (httpResponse.getStatus() == 404) {
      //asset read is not found then read from the cert/v1/read api
      httpResponse = templateReadResponse(requestContext, certServiceBaseUrl, certTemplateReadUrl, templateId);
      if (httpResponse.getStatus() == 404)
        throwClientErrorException(
                ResponseCode.RESOURCE_NOT_FOUND, "Given cert template not found: " + templateId);
    }
    if (StringUtils.isBlank(httpResponse.getBody())) {
      throwServerErrorException(
              ResponseCode.SERVER_ERROR, errorProcessingRequest.getErrorMessage());
    }
    return httpResponse.getBody();
  }

  private static HttpResponse<String> templateReadResponse(RequestContext requestContext, String baseUrl, String templateRelativeUrl, String templateId) throws Exception {
    String certTempUrl = getTemplateUrl(requestContext, baseUrl, templateRelativeUrl, templateId);
    HttpResponse<String> httpResponse = null;
    httpResponse = Unirest.get(certTempUrl).headers(getdefaultHeaders()).asString();
    logger.info(requestContext, "CourseBatchUtil:getResponse Response Status : " + httpResponse.getStatus());
    return httpResponse;
  }

  private static String getTemplateUrl(RequestContext requestContext, String baseUrl, String templateRelativeUrl, String templateId) {
    String certTempUrl = baseUrl + templateRelativeUrl + "/" + templateId + "?fields=certType,artifactUrl,issuer,signatoryList,name,data";
    logger.info(requestContext, "CourseBatchUtil:getTemplate certTempUrl : " + certTempUrl);
    return certTempUrl;
  }

  // Method will change the date variables into text with valid format
  public static Map<String, Object> esCourseMapping(CourseBatch courseBatch, String pattern) throws Exception {
    SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
    SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
    dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    Map<String, Object> esCourseMap = mapper.convertValue(courseBatch, Map.class);
    if (courseBatch.getStartTime() != null && courseBatch.getEndTime() != null) {
      esCourseMap.put(JsonKey.START_TIME, courseBatch.getStartDate());
      esCourseMap.put(JsonKey.END_TIME, courseBatch.getEndDate());
    }
    changeInDateFormat.forEach(key -> {
      if (null != esCourseMap.get(key))
        esCourseMap.put(key, dateTimeFormat.format(esCourseMap.get(key)));
      else 
        esCourseMap.put(key, null);
    });
    changeInSimpleDateFormat.forEach(key -> {
      if (null != esCourseMap.get(key))
        esCourseMap.put(key, dateFormat.format(esCourseMap.get(key)));
      else 
        esCourseMap.put(key, null);
    });
    esCourseMap.put(CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN, courseBatch.getCertTemplates());
    return esCourseMap;
  }

  // Method will change the timestamp (Long) into date with valid format
  public static Map<String, Object> cassandraCourseMapping(CourseBatch courseBatch, String pattern) {
    SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
    SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
    dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    Map<String, Object> courseBatchMap = mapper.convertValue(courseBatch, Map.class);
    changeInDateFormatAll.forEach(key -> {
      try {
        if (courseBatchMap.containsKey(key))
          courseBatchMap.put(key, setEndOfDay(key, dateTimeFormat.parse(dateTimeFormat.format(courseBatchMap.get(key))), dateFormat));
      } catch (ParseException e) {
        logger.error(null, "CourseBatchUtil:cassandraCourseMapping: Exception occurred with message = " + e.getMessage(), e);
      }
    });
    return courseBatchMap;
  }

  // Method will add endOfDay (23:59:59:999) in endDate and enrollmentEndDate
  private static Date setEndOfDay(String key, Date value, SimpleDateFormat dateFormat) {
    try {
      if (setEndOfDay.contains(key)) {
        Calendar cal =
                Calendar.getInstance(
                        TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
        cal.setTime(dateFormat.parse(dateFormat.format(value)));
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        return cal.getTime();
      }
    } catch (ParseException e) {
      logger.error(null, "CourseBatchUtil:setEndOfDay: Exception occurred with message = " + e.getMessage(), e);
    }
    return value;
  }

  public static Map<String, Object> cassandraEventMapping(EventBatch courseBatch, String pattern) {
    SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
    SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
    dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    Map<String, Object> courseBatchMap = mapper.convertValue(courseBatch, Map.class);
    changeInDateFormatAll.forEach(key -> {
      try {
        if (courseBatchMap.containsKey(key))
          courseBatchMap.put(key, setEndOfDay(key, dateTimeFormat.parse(dateTimeFormat.format(courseBatchMap.get(key))), dateFormat));
      } catch (ParseException e) {
        logger.error(null, "CourseBatchUtil:cassandraCourseMapping: Exception occurred with message = " + e.getMessage(), e);
      }
    });
    return courseBatchMap;
  }

  public static Map<String, Object> esEventMapping(EventBatch eventBatch, String pattern) throws Exception {
    SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
    SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
    dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    Map<String, Object> esCourseMap = mapper.convertValue(eventBatch, Map.class);
    if (eventBatch.getStartTime() != null && eventBatch.getEndTime() != null) {
      esCourseMap.put(JsonKey.START_TIME, eventBatch.getStartDate());
      esCourseMap.put(JsonKey.END_TIME, eventBatch.getEndDate());
    }
    changeInDateFormat.forEach(key -> {
      if (null != esCourseMap.get(key))
        esCourseMap.put(key, dateTimeFormat.format(esCourseMap.get(key)));
      else
        esCourseMap.put(key, null);
    });
    changeInSimpleDateFormat.forEach(key -> {
      if (null != esCourseMap.get(key))
        esCourseMap.put(key, dateFormat.format(esCourseMap.get(key)));
      else
        esCourseMap.put(key, null);
    });
    esCourseMap.put(CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN, eventBatch.getCertTemplates());
    return esCourseMap;
  }

  public static Map<String, Object> validateEventBatch(RequestContext requestContext, String eventId, String batchId) {
    Future<Map<String, Object>> resultF =
            esUtil.getDataByIdentifier(requestContext, EsType.courseBatch.getTypeName(), batchId);
    Map<String, Object> result =
            (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(resultF);
    if (MapUtils.isEmpty(result)) {
      ProjectCommonException.throwClientErrorException(
              ResponseCode.CLIENT_ERROR, "No such batchId exists");
    }
    if (StringUtils.isNotBlank(eventId)
            && !StringUtils.equals(eventId, (String) result.get(JsonKey.EVENT_ID_KEY))) {
      ProjectCommonException.throwClientErrorException(
              ResponseCode.CLIENT_ERROR, "batchId is not linked with eventId");
    }
    return result;
  }

  public static int syncBlendedProgramDurationCache(Map<String, Object> courseContent, String courseId, String batchId, RequestContext requestContext) {
    deleteBlendedBatchDurationCache(courseId, batchId, requestContext);
    return calculateBlendedProgramDuration(courseContent, courseId, batchId, requestContext);
  }

  public static int calculateBlendedProgramDuration(Map<String, Object> courseContent, String courseId, String batchId, RequestContext requestContext) {
    if (StringUtils.isBlank(courseId) || StringUtils.isBlank(batchId)) return 0;

    String blendedBatchDurationKey = getBlendedDurationKey(courseId, batchId);

    String cachedDuration = cacheUtil.get(blendedBatchDurationKey, null, 0);
    int duration = NumberUtils.toInt(cachedDuration, 0);

    if (duration > 0) {
      return duration;
    }

    Map<String, Object> hierarchyMap = getRedisHierarchyMap(courseId);
    Map<String, Object> batchAttrs = getBatchAttributesFromHierarchy(hierarchyMap, batchId);
    int offlineSessionsSum = MapUtils.isNotEmpty(batchAttrs) ? calculateOfflineSessionsSum(batchAttrs) : 0;

    if (offlineSessionsSum == 0 && MapUtils.isEmpty(hierarchyMap)) {
      try {
        CourseBatch batchData = courseBatchDao.readById(courseId, batchId, requestContext);
        if (batchData != null) {
          offlineSessionsSum = calculateOfflineSessionsSum(batchData.getBatchAttributes());
        }
      } catch (Exception e) {
        logger.error(requestContext, "Error reading batch details from DB for courseId: " + courseId, e);
      }
    }

    List<Map<String, Object>> children = getCourseChildren(courseId, hierarchyMap, courseContent, requestContext);
    int componentsDuration = calculateNestedComponentsDuration(children, getAssessmentDurationCourseCategories());

    int totalDuration = offlineSessionsSum + componentsDuration;

    try {
      cacheUtil.set(blendedBatchDurationKey, String.valueOf(totalDuration), 0);
    } catch (Exception e) {
      logger.warn(requestContext, "Error setting blended duration cache for key " + blendedBatchDurationKey + ": " + e.getMessage(), e);
    }
    return totalDuration;
  }

  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> getCourseChildren(String courseId, Map<String, Object> hierarchyMap,
          Map<String, Object> courseContent, RequestContext requestContext) {

    List<Map<String, Object>> children = Collections.emptyList();

    if (MapUtils.isNotEmpty(hierarchyMap)) {
      children = (List<Map<String, Object>>) hierarchyMap.get(JsonKey.CHILDREN);
    }

    if (CollectionUtils.isEmpty(children) && MapUtils.isNotEmpty(courseContent)) {
      children = (List<Map<String, Object>>) courseContent.get(JsonKey.CHILDREN);
    }

    if (CollectionUtils.isNotEmpty(children)) {
      return children;
    }

    try {
      children = contentHierarchyDao.getContentChildren(requestContext, courseId);
    } catch (Exception e) {
      logger.error(requestContext, "Error fetching children from DB for course " + courseId, e);
    }

    return children == null ? Collections.emptyList() : children;
  }

  public static int calculateOfflineSessionsSum(Map<String, Object> batchAttributes) {
    if (MapUtils.isEmpty(batchAttributes) || !batchAttributes.containsKey(JsonKey.SESSION_DETAILS_V2)) return 0;
    List<Map<String, Object>> sessionList = (List<Map<String, Object>>) batchAttributes.get(JsonKey.SESSION_DETAILS_V2);
    if (CollectionUtils.isEmpty(sessionList)) return 0;

    int sum = 0;
    for (Map<String, Object> session : sessionList) {

      if (session == null) {
        continue;
      }

      if (!JsonKey.OFFLINE_SESSION.equalsIgnoreCase(String.valueOf(session.get(JsonKey.SESSION_TYPE)))) {
        continue;
      }
      sum += parseSessionDurationAsSeconds(session.get(JsonKey.SESSION_DURATION));
    }
    return sum;
  }

  @SuppressWarnings("unchecked")
  private static int calculateNestedComponentsDuration(List<Map<String, Object>> nodes, Set<String> assessmentCategories) {

    if (CollectionUtils.isEmpty(nodes)) {
      return 0;
    }

    int totalDuration = 0;

    for (Map<String, Object> node : nodes) {

      if (node == null) {
        continue;
      }

      String category = String.valueOf(node.getOrDefault(JsonKey.PRIMARYCATEGORY, ""));

      if (JsonKey.LEARNING_RESOURCE.equalsIgnoreCase(category)) {
        totalDuration += getDurationAsInt(node, JsonKey.DURATION);
      } else if (assessmentCategories.contains(category)) {
        totalDuration += getDurationAsInt(node, JsonKey.EXPECTED_DURATION);
      }

      totalDuration += calculateNestedComponentsDuration((List<Map<String, Object>>) node.get(JsonKey.CHILDREN), assessmentCategories);
    }

    return totalDuration;
  }

  public static Map<String, Object> getBatchAttributesFromHierarchy(Map<String, Object> hierarchyMap, String batchId) {
    if (MapUtils.isEmpty(hierarchyMap) || !hierarchyMap.containsKey(JsonKey.BATCHES)) return Collections.emptyMap();
    List<Map<String, Object>> batchesList = (List<Map<String, Object>>) hierarchyMap.get(JsonKey.BATCHES);
    if (CollectionUtils.isEmpty(batchesList)) return Collections.emptyMap();

    for (Map<String, Object> b : batchesList) {
      if (b == null) continue;
      String bId = (String) b.get(JsonKey.BATCH_ID);
      if (StringUtils.equals(bId, batchId)) {
        Object attributes = b.get(JsonKey.BATCH_ATTRIBUTES);
        if (attributes == null) {
          attributes = b.get(JsonKey.BATCH_ATTRIBUTES_KEY);
        }
        return attributes instanceof Map ? (Map<String, Object>) attributes : Collections.emptyMap();
      }
    }
    return Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getRedisHierarchyMap(String courseId) {
    if (StringUtils.isBlank(courseId)) {
      return Collections.emptyMap();
    }
    int ttl = NumberUtils.toInt(PropertiesCache.getInstance().getProperty(JsonKey.CONTENT_TTL), 0);
    String json = cacheUtil.getUsingIndex(JsonKey.HIERARCHY + "_" + courseId, null, ttl, 0);

    if (StringUtils.isBlank(json)) {
      return Collections.emptyMap();
    }

    try {
      return mapper.readValue(json, Map.class);
    } catch (Exception e) {
      logger.error(null, "Error reading hierarchy json from redis for courseId: " + courseId, e);
      return Collections.emptyMap();
    }
  }

  private static String getBlendedDurationKey(String courseId, String batchId) {
    return JsonKey.BLENDED + ':' + courseId + ':' + batchId + ':' + JsonKey.DURATION;
  }

  private static Set<String> getAssessmentDurationCourseCategories() {
    String categories = ProjectUtil.getConfigValue(JsonKey.LEARNING_HOURS_ASSESSMENT_COURSE_CATEGORIES);

    if (StringUtils.isBlank(categories)) {
      return Collections.emptySet();
    }

    return Arrays.stream(categories.split(",")).map(String::trim).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
  }

  public static void deleteBlendedBatchDurationCache(String courseId, String batchId, RequestContext requestContext) {
    if (StringUtils.isBlank(courseId) || StringUtils.isBlank(batchId)) return;
    String blendedBatchDurationKey = getBlendedDurationKey(courseId, batchId);
    try {
      cacheUtil.deleteKey(blendedBatchDurationKey);
    } catch (Exception e) {
      logger.warn(requestContext, "Error deleting blended duration cache for key " + blendedBatchDurationKey + ": " + e.getMessage(), e);
    }
  }

  public static int getDurationAsInt(Map<String, Object> content, String key) {
    if (MapUtils.isEmpty(content)) {
      return 0;
    }

    Object value = content.get(key);
    if (value == null) {
      return 0;
    }

    if (value instanceof Number) {
      return ((Number) value).intValue();
    }

    return NumberUtils.toInt(value.toString().trim(), 0);
  }

  public static int parseSessionDurationAsSeconds(Object durationObj) {
    if (durationObj == null) {
      return 0;
    }

    if (durationObj instanceof Number) {
      return ((Number) durationObj).intValue() * 60;
    }

    String duration = durationObj.toString().trim();
    if (StringUtils.isBlank(duration)) {
      return 0;
    }

    if (StringUtils.isNumeric(duration)) {
      return NumberUtils.toInt(duration, 0) * 60;
    }

    int seconds = 0;

    Matcher matcher = HOUR_REGEX.matcher(duration);
    if (matcher.find()) {
      seconds += NumberUtils.toInt(matcher.group(1), 0) * 3600;
    }

    matcher = MINUTE_REGEX.matcher(duration);
    if (matcher.find()) {
      seconds += NumberUtils.toInt(matcher.group(1), 0) * 60;
    }

    return seconds;
  }

}
