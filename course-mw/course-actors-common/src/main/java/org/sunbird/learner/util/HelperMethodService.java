package org.sunbird.learner.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.util.JsonUtil;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.redis.RedisCache;

import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.stream.Collectors;

public class HelperMethodService {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static LoggerUtil logger = new LoggerUtil(HelperMethodService.class);

    private final CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private final RedisCache cacheService = new RedisCache();

    public String fetchDataForKeys(String key) {
        return cacheService.getCache(key);
    }

    public List<Object> fetchUserFromPrimary(String userId, RequestContext requestContext) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(JsonKey.ID, userId);

        Response resp = cassandraOperation.getRecordsByProperties(
                JsonKey.KEYSPACE_SUNBIRD,
                JsonKey.USER,
                propertyMap,
                Arrays.asList(JsonKey.FIRST_NAME, JsonKey.ID),
                requestContext
        );

        if (resp == null || MapUtils.isEmpty(resp.getResult())) {
            return Collections.emptyList();
        }

        Object rowsObj = resp.getResult().get(JsonKey.RESPONSE);
        if (rowsObj == null) {
            rowsObj = resp.getResult().get(JsonKey.RESPONSE);
            if (rowsObj == null) {
                rowsObj = resp.getResult().get(JsonKey.RECORDS);
            }
        }

        List<Map<String, Object>> userInfoList = Collections.emptyList();
        if (rowsObj instanceof List) {
            userInfoList = (List<Map<String, Object>>) rowsObj;
        }

        if (CollectionUtils.isEmpty(userInfoList)) {
            return Collections.emptyList();
        }

        return userInfoList.stream()
                .map(userInfo -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put(JsonKey.USER_ID_REQ, userInfo.get(JsonKey.ID));
                    m.put(JsonKey.FIRST_NAME_KEY, userInfo.get(JsonKey.FIRST_NAME));
                    return (Object) m;
                })
                .collect(Collectors.toList());
    }

    public String fetchUserFirstName(String userId, RequestContext requestContext) {
        String redisResults = fetchDataForKeys(JsonKey.USER_PREFIX + userId);
        if (StringUtils.isNotBlank(redisResults)) {
            try {
                Map<String, Object> resultMap =
                        objectMapper.readValue(redisResults, new TypeReference<Map<String, Object>>() {
                        });
                Object nameObj = resultMap.get(JsonKey.FIRST_NAME_KEY);
                if (nameObj instanceof String && StringUtils.isNotBlank((String) nameObj)) {
                    return (String) nameObj;
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        List<Object> cassandraResults = fetchUserFromPrimary(userId, requestContext);
        if (CollectionUtils.isNotEmpty(cassandraResults) && cassandraResults.get(0) instanceof Map) {
            String name = (String) ((Map<?, ?>) cassandraResults.get(0)).get(JsonKey.FIRST_NAME_KEY);
            if (StringUtils.isNotBlank(name)) {
                return name;
            }
        }
        return "User";
    }


    public void sendNotificationToMDOs(String eventId, String userId, RequestContext requestContext) {
        logger.debug(requestContext, "sendNotificationToMDOs called with eventId: " + eventId + ", userId: " + userId);
        String firstName = fetchUserFirstName(userId, requestContext);
        Map<String, Object> response = ContentUtil.getEventContent(eventId, Arrays.asList(JsonKey.NAME, JsonKey.CREATED_BY, JsonKey.START_DATE, JsonKey.COURSE_CREATED_FOR));
        Map<String, Object> eventDetails = (Map<String, Object>) response.get(JsonKey.CONTENT);
        List<String> filteredUserIdList = ContentUtil.fetchMdoList((List<String>) eventDetails.get(JsonKey.COURSE_CREATED_FOR), eventDetails.get(JsonKey.CREATED_BY).toString(), requestContext);
        Map<String, Object> notificationData = Map.of(JsonKey.ID, eventId);
        if (CollectionUtils.isNotEmpty(filteredUserIdList)) {
            filteredUserIdList = filteredUserIdList.stream()
                    .filter(user -> !user.equalsIgnoreCase(userId))
                    .collect(Collectors.toList());
            triggerNotification(JsonKey.EVENT_ENROLLED, JsonKey.ALERT, filteredUserIdList, firstName,
                    eventDetails.get(JsonKey.NAME).toString(), eventDetails.get(JsonKey.START_DATE).toString(), notificationData, requestContext);
        } else {
            logger.info(requestContext, "No MDO leaders found for event: " + eventId);
        }
    }

    public void triggerNotification(
            String subCategory,
            String subType,
            List<String> userIds,
            String userName,
            String title,
            String startDate,
            Map<String, Object> data,
            RequestContext requestContext
    ) {
        ObjectNode placeholders = objectMapper.createObjectNode();
        placeholders.put(JsonKey.EVENT_NAME, title);
        placeholders.put(JsonKey.USERNAME, userName);
        placeholders.put(JsonKey.DATE_KEY, startDate);

        Map<String, Object> message = new HashMap<>();
        message.put(JsonKey.PLACE_HOLDERS, placeholders);
        message.put(JsonKey.DATA, data);

        try {
            sendNotification(subCategory, subType, userIds, message);
            logger.info(requestContext, "Notification sent successfully for subCategory: " + subCategory);
        } catch (Exception e) {
            logger.error(requestContext, "Notification failed for subCategory: " + subCategory, e);
        }
    }

    public void sendNotification(
            String subCategory,
            String subType,
            List<String> userIds,
            Map<String, Object> message
    ) {
        try {
            if (StringUtils.isBlank(subCategory)) {
                logger.error(null, "subCategory is required", null);
            }
            if (StringUtils.isBlank(subType)) {
                logger.error(null, "subType is required", null);
            }

            if (CollectionUtils.isEmpty(userIds)) {
                logger.error(null, "userIds cannot be null or empty", null);
            }

            if (MapUtils.isEmpty(message)) {
                logger.error(null, "message cannot be null or empty", null);
            }

            Map<String, Object> payload = new HashMap<>();
            payload.put(JsonKey.SUB_CATEGORY, subCategory);
            payload.put(JsonKey.SUB_TYPE, subType);
            payload.put(JsonKey.USER_ID_KEYS, userIds);
            payload.put(JsonKey.MESSAGE, message);

            String requestJson = JsonUtil.serialize(payload);
            String contentUpdateBaseUrl = ProjectUtil.getConfigValue(JsonKey.NOTIFICATION_WRAPPER_API_HOST) + ProjectUtil.getConfigValue(JsonKey.NOTIFICATION_WRAPPER_API_ENDPOINT);

            Map<String, String> headers = new HashMap<>();
            headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
            logger.debug(null, "Sending notification with payload: " + requestJson);
            HttpUtil.sendPostRequest(contentUpdateBaseUrl, requestJson, headers);
            logger.debug(null, "Notification sent successfully to users: " + userIds);
        } catch (IllegalArgumentException iae) {
            logger.error(null, "Invalid input for sendNotification: {}", iae);
        } catch (Exception e) {
            logger.error(null, "Unexpected error while sending notification: {}", e);
        }
    }
}
