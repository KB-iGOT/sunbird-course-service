package org.sunbird.learner.util;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.sunbird.common.exception.ProjectCommonException.throwClientErrorException;
import static org.sunbird.common.exception.ProjectCommonException.throwServerErrorException;
import static org.sunbird.common.models.util.JsonKey.BEARER;
import static org.sunbird.common.models.util.JsonKey.SUNBIRD_AUTHORIZATION;
import static org.sunbird.common.models.util.ProjectUtil.getConfigValue;
import static org.sunbird.common.responsecode.ResponseCode.errorProcessingRequest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.sunbird.models.event.batch.EventBatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;

import scala.concurrent.Future;

public class EventBatchUtil {
    private static ElasticSearchService esUtil = EsClientFactory.getInstance(JsonKey.REST);
    private static ObjectMapper mapper = new ObjectMapper();
    private static LoggerUtil logger = new LoggerUtil(CourseBatchUtil.class);
    private static final List<String> changeInDateFormat = JsonKey.CHANGE_IN_DATE_FORMAT;
    private static final List<String> changeInSimpleDateFormat = JsonKey.CHANGE_IN_SIMPLE_DATE_FORMAT;
    private static final List<String> changeInDateFormatAll = JsonKey.CHANGE_IN_DATE_FORMAT_ALL;
    private static final List<String> setEndOfDay = JsonKey.SET_END_OF_DAY;

    private EventBatchUtil() {
    }

    public static void syncEventBatchForeground(RequestContext requestContext, String uniqueId,
            Map<String, Object> req) {
        logger.info(requestContext,
                "EventBatchManagementActor: syncEventBatchForeground called for event batch ID = "
                        + uniqueId);
        req.put(JsonKey.ID, uniqueId);
        req.put(JsonKey.IDENTIFIER, uniqueId);
        Future<String> esResponseF = esUtil.save(requestContext, ProjectUtil.EsType.eventBatch.getTypeName(), uniqueId,
                req);
        String esResponse = (String) ElasticSearchHelper.getResponseFromFuture(esResponseF);
        logger.info(requestContext,
                "EventBatchManagementActor::syncEventBatchForeground: Sync response for event batch ID = "
                        + uniqueId
                        + " received response = "
                        + esResponse);
    }

    public static Map<String, Object> validateEventBatch(RequestContext requestContext, String eventId,
            String batchId) {
        Future<Map<String, Object>> resultF = esUtil.getDataByIdentifier(requestContext,
                EsType.eventBatch.getTypeName(), batchId);
        Map<String, Object> result = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(resultF);
        if (MapUtils.isEmpty(result)) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.CLIENT_ERROR, "No such batchId exists");
        }
        if (StringUtils.isNotBlank(eventId)
                && !StringUtils.equals(eventId, (String) result.get(JsonKey.COURSE_ID))) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.CLIENT_ERROR, "batchId is not linked with eventId");
        }
        return result;
    }

    public static Map<String, Object> validateTemplate(RequestContext requestContext, String templateId) {
        Response templateResponse = getTemplate(requestContext, templateId);
        if (templateResponse == null
                || MapUtils.isEmpty(templateResponse.getResult())
                || !(templateResponse.getResult().containsKey(JsonKey.CONTENT)
                        || templateResponse.getResult().containsKey("certificate"))) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.CLIENT_ERROR, "Invalid template Id: " + templateId);
        }
        Map<String, Object> template = templateResponse.getResult().containsKey(JsonKey.CONTENT)
                ? (Map<String, Object>) templateResponse.getResult().getOrDefault(JsonKey.CONTENT, new HashMap<>())
                : (Map<String, Object>) ((Map<String, Object>) templateResponse.getResult().getOrDefault("certificate",
                        new HashMap<>())).getOrDefault(JsonKey.TEMPLATE, new HashMap<>());

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
                    "EventBatchUtil:getResponse ProjectCommonException:"
                            + "Request , Status : "
                            + e.getCode()
                            + " "
                            + e.getMessage()
                            + ",Response Body :"
                            + responseBody,
                    e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(requestContext,
                    "EventBatchUtil:getResponse occurred with error message = "
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
            // asset read is not found then read from the cert/v1/read api
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

    private static HttpResponse<String> templateReadResponse(RequestContext requestContext, String baseUrl,
            String templateRelativeUrl, String templateId) throws Exception {
        String certTempUrl = getTemplateUrl(requestContext, baseUrl, templateRelativeUrl, templateId);
        HttpResponse<String> httpResponse = null;
        httpResponse = Unirest.get(certTempUrl).headers(getdefaultHeaders()).asString();
        logger.info(requestContext, "EventBatchUtil:getResponse Response Status : " + httpResponse.getStatus());
        return httpResponse;
    }

    private static String getTemplateUrl(RequestContext requestContext, String baseUrl, String templateRelativeUrl,
            String templateId) {
        String certTempUrl = baseUrl + templateRelativeUrl + "/" + templateId
                + "?fields=certType,artifactUrl,issuer,signatoryList,name,data";
        logger.info(requestContext, "EventBatchUtil:getTemplate certTempUrl : " + certTempUrl);
        return certTempUrl;
    }

    // Method will add endOfDay (23:59:59:999) in endDate and enrollmentEndDate
    private static Date setEndOfDay(String key, Date value, SimpleDateFormat dateFormat) {
        try {
            if (setEndOfDay.contains(key)) {
                Calendar cal = Calendar.getInstance(
                        TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
                cal.setTime(dateFormat.parse(dateFormat.format(value)));
                cal.set(Calendar.HOUR_OF_DAY, 23);
                cal.set(Calendar.MINUTE, 59);
                cal.set(Calendar.SECOND, 59);
                cal.set(Calendar.MILLISECOND, 999);
                return cal.getTime();
            }
        } catch (ParseException e) {
            logger.error(null, "EventBatchUtil:setEndOfDay: Exception occurred with message = " + e.getMessage(), e);
        }
        return value;
    }

    public static Map<String, Object> cassandraEventMapping(EventBatch eventBatch, String pattern) {
        SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
        SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
        dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
        Map<String, Object> eventBatchMap = mapper.convertValue(eventBatch, Map.class);
        changeInDateFormatAll.forEach(key -> {
            try {
                if (eventBatchMap.containsKey(key))
                    eventBatchMap.put(key, setEndOfDay(key,
                            dateTimeFormat.parse(dateTimeFormat.format(eventBatchMap.get(key))), dateFormat));
            } catch (ParseException e) {
                logger.error(null,
                        "EventBatchUtil:cassandraEventMapping: Exception occurred with message = " + e.getMessage(),
                        e);
            }
        });
        return eventBatchMap;
    }

    public static Map<String, Object> esEventMapping(EventBatch eventBatch, String pattern) throws Exception {
        SimpleDateFormat dateFormat = ProjectUtil.getDateFormatter(pattern);
        SimpleDateFormat dateTimeFormat = ProjectUtil.getDateFormatter();
        dateFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
        Map<String, Object> esEventMap = mapper.convertValue(eventBatch, Map.class);
        if (eventBatch.getStartTime() != null && eventBatch.getEndTime() != null) {
            esEventMap.put(JsonKey.START_TIME, eventBatch.getStartDate());
            esEventMap.put(JsonKey.END_TIME, eventBatch.getEndDate());
        }
        changeInDateFormat.forEach(key -> {
            if (null != esEventMap.get(key))
                esEventMap.put(key, dateTimeFormat.format(esEventMap.get(key)));
            else
                esEventMap.put(key, null);
        });
        changeInSimpleDateFormat.forEach(key -> {
            if (null != esEventMap.get(key))
                esEventMap.put(key, dateFormat.format(esEventMap.get(key)));
            else
                esEventMap.put(key, null);
        });
        esEventMap.put(CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN, eventBatch.getCertTemplates());
        return esEventMap;
    }

}
