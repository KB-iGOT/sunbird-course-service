package org.sunbird.learner.actors.eventbatch.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.eventbatch.EventBatchDao;
import org.sunbird.learner.constants.CourseJsonKey;
import org.sunbird.learner.util.EventBatchUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.event.batch.EventBatch;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class EventBatchDaoImpl implements EventBatchDao {
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo eventBatchDb = Util.dbInfoMap.get(JsonKey.EVENT_BATCH_DB);
    private ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(EventBatchDaoImpl.class);
    private String dateFormat = "yyyy-MM-dd";

    @Override
    public EventBatch readById(String eventId, String batchId, RequestContext requestContext) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(JsonKey.EVENT_ID, eventId);
        primaryKey.put(JsonKey.BATCH_ID, batchId);
        Response courseBatchResult =
                cassandraOperation.getRecordByIdentifier(
                        requestContext, eventBatchDb.getKeySpace(), eventBatchDb.getTableName(), primaryKey,null);
        List<Map<String, Object>> eventList =
                (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
        if (eventList.isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidEventBatchId.getErrorCode(),
                    ResponseCode.invalidEventBatchId.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        } else {
            eventList.get(0).remove(JsonKey.PARTICIPANT);
            return mapper.convertValue(eventList.get(0), EventBatch.class);
        }
    }

    @Override
    public Response create(RequestContext requestContext, EventBatch eventBatch) {
        Map<String, Object> map = EventBatchUtil.cassandraEventMapping(eventBatch, dateFormat);
        map = CassandraUtil.changeCassandraColumnMapping(map);
        CassandraUtil.convertMaptoJsonString(map, JsonKey.BATCH_ATTRIBUTES_KEY);
        if(map.get(JsonKey.START_TIME) != null) {
            String dateType=JsonKey.START_DATE_BATCH ;
            String timeType=JsonKey.START_TIME;
            processStartEndDate(map, timeType, dateType);
            map.remove(JsonKey.START_TIME);
            eventBatch.setStartDate((Date)map.get(dateType));
        }
        if(map.get(JsonKey.END_TIME) != null) {
            String dateType=JsonKey.END_DATE_BATCH;
            String timeType=JsonKey.END_TIME;
            processStartEndDate(map, timeType, dateType);
            map.remove(JsonKey.END_TIME);
            eventBatch.setEndDate((Date)map.get(dateType));
        }
        return cassandraOperation.insertRecord(
                requestContext, eventBatchDb.getKeySpace(), eventBatchDb.getTableName(), map);
    }

    /**
     * This method processes the start and end date by merging the time part from the given map
     * into the date part from the given map and setting it to the specified timezone.
     *
     * @param map      The map containing the time and date information.
     * @param timeType The key in the map for the time string.
     * @param dateType The key in the map for the date object.
     */
    private static void processStartEndDate(Map<String, Object> map, String timeType, String dateType) {
        String timeStr = (String) map.get(timeType);
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
        Date time;
        try {
            time = timeFormat.parse(timeStr);
        } catch (ParseException e) {
            log.error("Failed to parse time string: " + timeStr, e);
            return;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime((Date) map.get(dateType));
        Calendar timeCalendar = Calendar.getInstance();
        timeCalendar.setTime(time);
        String timeZone = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE);
        TimeZone tz = TimeZone.getTimeZone(timeZone);
        calendar.setTimeZone(tz);
        log.info("Merging time part {} into date {} with timezone {}", timeStr, map.get(dateType), timeZone);
        calendar.set(Calendar.HOUR_OF_DAY, timeCalendar.get(Calendar.HOUR_OF_DAY));
        calendar.set(Calendar.MINUTE, timeCalendar.get(Calendar.MINUTE));
        calendar.set(Calendar.SECOND, timeCalendar.get(Calendar.SECOND));
        if(ProjectUtil.getConfigValue(JsonKey.ADD_EXTRA_HOURS_MINS).equalsIgnoreCase("true")){
            calendar.add(Calendar.HOUR_OF_DAY, 5);
            calendar.add(Calendar.MINUTE, 30);
            log.info("Added 5hours 30mins to the start_date and end_date");
        }
        map.put(dateType, calendar.getTime());
        log.info("Updated date in map with key {}: {}", dateType, calendar.getTime());
    }

    @Override
  public void addCertificateTemplateToEventBatch(
          RequestContext requestContext, String eventId, String batchId, String templateId, Map<String, Object> templateDetails) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.EVENT_ID, eventId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    cassandraOperation.updateAddMapRecord(
            requestContext, eventBatchDb.getKeySpace(),
        eventBatchDb.getTableName(),
        primaryKey,
        CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN,
        templateId,
        templateDetails);
  }

  @Override
  public void removeCertificateTemplateFromEventBatch(
          RequestContext requestContext, String eventId, String batchId, String templateId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.EVENT_ID, eventId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    cassandraOperation.updateRemoveMapRecord(
            requestContext, eventBatchDb.getKeySpace(),
        eventBatchDb.getTableName(),
        primaryKey,
        CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN,
        templateId);
  }

  @Override
  public Map<String, Object> getEventBatch(RequestContext requestContext, String eventId, String batchId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.COURSE_ID, eventId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    Response eventBatchResult =
        cassandraOperation.getRecordByIdentifier(
                requestContext, eventBatchDb.getKeySpace(), eventBatchDb.getTableName(), primaryKey, null);
    List<Map<String, Object>> eventList =
        (List<Map<String, Object>>) eventBatchResult.get(JsonKey.RESPONSE);
    return eventList.get(0);
  }
}
