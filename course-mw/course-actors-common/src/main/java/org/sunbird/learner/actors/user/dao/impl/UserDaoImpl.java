package org.sunbird.learner.actors.user.dao.impl;

import java.util.HashMap;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.RequestContext;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

public class UserDaoImpl {
    private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private static final String KEYSPACE_NAME = Util.dbInfoMap.get(JsonKey.USER_INFO_DB).getKeySpace();
    private static final String TABLE_NAME = Util.dbInfoMap.get(JsonKey.USER_INFO_DB).getTableName();

    public Response read(String userId, RequestContext requestContext) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.ID, userId);
        return cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, TABLE_NAME, primaryKey, null);
    }
}
