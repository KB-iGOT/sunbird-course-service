package org.sunbird.learner.actors.event;


import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;


import java.util.List;
import java.util.Map;

public interface EventEnrolmentDao {
    List<Map<String,Object>> getEnrolmentList(Request request, String userId);

    List<Map<String, Object>> getUserEventEnrollment(Request request, String userId,String eventId ,String batchId);

    List<Map<String, Object>> getUserEventState(Request request);

    Map<String, Object> getUserDetails(String userId, RequestContext requestContext);

    List<Map<String,Object>> getEventEnrolmentList(Request request, String userId);

    List<Map<String, Object>> getEnrolmentListV2(Request request, String userId);

}
