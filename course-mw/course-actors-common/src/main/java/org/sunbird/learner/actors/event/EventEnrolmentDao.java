package org.sunbird.learner.actors.event;


import org.sunbird.common.request.Request;


import java.util.List;
import java.util.Map;

public interface EventEnrolmentDao {
    List<Map<String,Object>> getEnrolmentList(Request request, String userId);
    List<Map<String,Object>> getBatchList(Request request,String evenId,String batchId);
    List<Map<String, Object>> getUserEventEnrollment(Request request, String userId,String eventId ,String batchId);
}
