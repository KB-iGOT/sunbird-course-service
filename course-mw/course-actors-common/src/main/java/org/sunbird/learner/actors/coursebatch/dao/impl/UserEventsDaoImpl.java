package org.sunbird.learner.actors.coursebatch.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.request.RequestContext;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.dao.UserEventsDao;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.courses.UserCourses;
import org.sunbird.models.user.events.UserEvents;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserEventsDaoImpl implements UserEventsDao {
    protected LoggerUtil logger = new LoggerUtil(this.getClass());
    private ObjectMapper mapper = new ObjectMapper();
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private static final String KEYSPACE_NAME =
            Util.dbInfoMap.get(JsonKey.USER_EVENT_DB).getKeySpace();
    private static final String TABLE_NAME =
            Util.dbInfoMap.get(JsonKey.USER_EVENT_DB).getTableName();

    private static final String USER_EVENTS_ENROLLMENTS = Util.dbInfoMap.get(JsonKey.USER_EVENT_DB).getTableName();

    @Override
    public UserEvents read(RequestContext requestContext, String userId, String eventId, String batchId) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.USER_ID, userId);
        primaryKey.put(JsonKey.CONTENT_ID,eventId);
        primaryKey.put(JsonKey.CONTEXT_ID,eventId);
        primaryKey.put(JsonKey.BATCH_ID, batchId);
        Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, TABLE_NAME, primaryKey, null);
        List<Map<String, Object>> userEventList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if (CollectionUtils.isEmpty(userEventList)) {
            return null;
        }
        try {
            return mapper.convertValue((Map<String, Object>) userEventList.get(0), UserEvents.class);
        } catch (Exception e) {
            logger.error(requestContext, "Failed to read user enrollments table. Exception: ", e);
        }
        return null;
    }

    @Override
    public Response insertV2(RequestContext requestContext, Map<String, Object> userEventDetails) {
        return cassandraOperation.insertRecord(requestContext, KEYSPACE_NAME, USER_EVENTS_ENROLLMENTS, userEventDetails);
    }

    @Override
    public Response updateV2(RequestContext requestContext, String userId, String eventId, String batchId, Map<String, Object> updateAttributes) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.USER_ID, userId);
        primaryKey.put(JsonKey.CONTENT_ID, eventId);
        primaryKey.put(JsonKey.CONTEXT_ID, eventId);
        primaryKey.put(JsonKey.BATCH_ID, batchId);
        Map<String, Object> updateList = new HashMap<>();
        updateList.putAll(updateAttributes);
        updateList.remove(JsonKey.BATCH_ID_KEY);
        updateList.remove(JsonKey.CONTENT_ID_KEY);
        updateList.remove(JsonKey.CONTEXT_ID_KEY);
        updateList.remove(JsonKey.USER_ID_KEY);
        return cassandraOperation.updateRecord(requestContext, KEYSPACE_NAME, USER_EVENTS_ENROLLMENTS, updateList, primaryKey);
    }
}
