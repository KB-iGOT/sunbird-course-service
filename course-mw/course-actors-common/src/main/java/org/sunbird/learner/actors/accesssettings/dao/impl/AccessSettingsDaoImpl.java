package org.sunbird.learner.actors.accesssettings.dao.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.request.RequestContext;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.accesssettings.model.AccessControl;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.sunbird.common.responsecode.ResponseCode;

public class AccessSettingsDaoImpl {
    private LoggerUtil logger = new LoggerUtil(AccessSettingsDaoImpl.class);
    private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private static final String KEYSPACE_NAME = Util.dbInfoMap.get(JsonKey.ACCESS_SETTINGS_DB).getKeySpace();
    private static final String TABLE_NAME = Util.dbInfoMap.get(JsonKey.ACCESS_SETTINGS_DB).getTableName();

    private static AccessSettingsDaoImpl instance;
    private ObjectMapper objectMapper = new ObjectMapper();

    public static AccessSettingsDaoImpl getInstance() {
        if (instance == null) {
            synchronized (AccessSettingsDaoImpl.class) {
                if (instance == null) {
                    instance = new AccessSettingsDaoImpl();
                }
            }
        }
        return instance;
    }

    private AccessSettingsDaoImpl() {
        // Private constructor to prevent instantiation
    }

    public AccessControl readAccessSettings(RequestContext requestContext, String courseId) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.CONTEXT_ID, courseId);
        Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, TABLE_NAME,
                primaryKey, null);
        try {
            if (response != null && response.getResponseCode() == ResponseCode.OK) {
                List<Map<String, Object>> accessSettingsList = (List<Map<String, Object>>) response
                        .get(JsonKey.RESPONSE);
                if (CollectionUtils.isNotEmpty(accessSettingsList)) {
                    Map<String, Object> dbRecord = accessSettingsList.get(0);
                    Map<String, Object> contextData = objectMapper
                            .readValue((String) dbRecord.get(JsonKey.CONTEXT_DATA), Map.class);
                    if (contextData.containsKey(JsonKey.ACCESS_CONTROL)) {
                        // Deserialize accessControl JSON to AccessControl object
                        return objectMapper.convertValue(contextData.get(JsonKey.ACCESS_CONTROL), AccessControl.class);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(requestContext, "Failed to read access settings for courseId: " + courseId, e);
        }
        return null;
    }
}
