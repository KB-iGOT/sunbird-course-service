package org.sunbird.learner.actors.coursebatch.dao.impl;

import com.datastax.driver.core.ConsistencyLevel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.CassandraPropertyReader;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.dao.CourseBatchDao;
import org.sunbird.learner.constants.CourseJsonKey;
import org.sunbird.learner.util.CourseBatchCacheUtil;
import org.sunbird.learner.util.CourseBatchUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.course.batch.CourseBatch;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.sunbird.common.models.util.ProjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CourseBatchDaoImpl implements CourseBatchDao {
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo courseBatchDb = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
  private static final CassandraPropertyReader propertiesCache =
          CassandraPropertyReader.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private String dateFormat = "yyyy-MM-dd";
  private static final Logger log = LoggerFactory.getLogger(CourseBatchDaoImpl.class);

  @Override
  public Response create(RequestContext requestContext, CourseBatch courseBatch) {
    Map<String, Object> map = CourseBatchUtil.cassandraCourseMapping(courseBatch, dateFormat);
    map = CassandraUtil.changeCassandraColumnMapping(map);
    CassandraUtil.convertMaptoJsonString(map, JsonKey.BATCH_ATTRIBUTES_KEY);
      if(map.get(JsonKey.START_TIME) != null) {
          String dateType=JsonKey.START_DATE_BATCH ;
          String timeType=JsonKey.START_TIME;
          processStartEndDate(map, timeType, dateType);
          map.remove(JsonKey.START_TIME);
          courseBatch.setStartDate((Date)map.get(dateType));
      }
      if(map.get(JsonKey.END_TIME) != null) {
          String dateType=JsonKey.END_DATE_BATCH;
          String timeType=JsonKey.END_TIME;
          processStartEndDate(map, timeType, dateType);
          map.remove(JsonKey.END_TIME);
          courseBatch.setEndDate((Date)map.get(dateType));
      }
    Response response = cassandraOperation.insertRecord(
            requestContext, courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), map);
    refreshCache(requestContext, courseBatch.getCourseId(), courseBatch.getBatchId());
    return response;
  }

  @Override
  public Response update(RequestContext requestContext, String courseId, String batchId, Map<String, Object> map) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.putAll(map);
    attributeMap.remove(JsonKey.COURSE_ID);
    attributeMap.remove(JsonKey.BATCH_ID);
    attributeMap = CassandraUtil.changeCassandraColumnMapping(attributeMap);
    CassandraUtil.convertMaptoJsonString(attributeMap, JsonKey.BATCH_ATTRIBUTES_KEY);
    Response response = cassandraOperation.updateRecord(
            requestContext, courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), attributeMap, primaryKey);
    CourseBatchCacheUtil.evict(courseId, batchId);
    refreshCache(requestContext, courseId, batchId);
    return response;
  }

  /** Raw course_batch row from Cassandra (participant removed), or null when absent. */
  private Map<String, Object> readRow(RequestContext requestContext, String courseId, String batchId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    Response courseBatchResult =
        cassandraOperation.getRecordByIdentifier(
                requestContext, courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), primaryKey, null);
    List<Map<String, Object>> courseList =
        (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(courseList)) return null;
    Map<String, Object> row = courseList.get(0);
    row.remove(JsonKey.PARTICIPANT);
    return row;
  }

  /** Redis-first row read: Redis -> Cassandra (then cached). Null when the batch does not exist. */
  private Map<String, Object> readRowCached(RequestContext requestContext, String courseId, String batchId) {
    Map<String, Object> row = CourseBatchCacheUtil.get(courseId, batchId);
    if (row != null) return row;
    row = readRow(requestContext, courseId, batchId);
    if (row != null) CourseBatchCacheUtil.put(row);
    return row;
  }

  /** After a successful write: re-read the row and store it so the next read is warm and exact. */
  private void refreshCache(RequestContext requestContext, String courseId, String batchId) {
    if (!CourseBatchCacheUtil.isEnabled()) return;
    try {
      Map<String, Object> row = readRow(requestContext, courseId, batchId);
      if (row != null) CourseBatchCacheUtil.put(row);
    } catch (Exception e) {
      log.error("CourseBatchDaoImpl: cache refresh failed for {}/{}", courseId, batchId, e);
    }
  }

  @Override
  public CourseBatch readById(String courseId, String batchId, RequestContext requestContext) {
    Map<String, Object> row = readRowCached(requestContext, courseId, batchId);
    if (row == null) {
      throw new ProjectCommonException(
          ResponseCode.invalidCourseBatchId.getErrorCode(),
          ResponseCode.invalidCourseBatchId.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    return mapper.convertValue(row, CourseBatch.class);
  }

  @Override
  public Map<String, Object> getCourseBatch(RequestContext requestContext, String courseId, String batchId) {
    Map<String, Object> row = readRowCached(requestContext, courseId, batchId);
    if (row == null) throw new IndexOutOfBoundsException("No course_batch row for " + courseId + "/" + batchId);
    return row;
  }

  @Override
  public Response delete(RequestContext requestContext, String id) {
    return cassandraOperation.deleteRecord(
        courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), id, requestContext);
  }

  @Override
  public void addCertificateTemplateToCourseBatch(
          RequestContext requestContext, String courseId, String batchId, String templateId, Map<String, Object> templateDetails) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    cassandraOperation.updateAddMapRecord(
            requestContext, courseBatchDb.getKeySpace(),
        courseBatchDb.getTableName(),
        primaryKey,
        CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN,
        templateId,
        templateDetails);
    CourseBatchCacheUtil.evict(courseId, batchId);
    refreshCache(requestContext, courseId, batchId);
  }

  @Override
  public void removeCertificateTemplateFromCourseBatch(
          RequestContext requestContext, String courseId, String batchId, String templateId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    cassandraOperation.updateRemoveMapRecord(
            requestContext, courseBatchDb.getKeySpace(),
        courseBatchDb.getTableName(),
        primaryKey,
        CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN,
        templateId);
    CourseBatchCacheUtil.evict(courseId, batchId);
    refreshCache(requestContext, courseId, batchId);
  }

  @Override
  public CourseBatch readFirstAvailableBatch(String courseId, RequestContext requestContext) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    Response courseBatchResult =
            cassandraOperation.getRecordByIdentifier(
                    requestContext, courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), primaryKey, null);
    List<Map<String, Object>> batchList =
            (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(batchList)) {
      throw new ProjectCommonException(
              ResponseCode.courseDoesNotHaveBatch.getErrorCode(),
              ResponseCode.courseDoesNotHaveBatch.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
    } else {
      List<Map<String, Object>> activeBatchList = batchList.stream()
              .filter(batch -> ((Integer) batch.get(JsonKey.STATUS)) != 2) // Filter batches where STATUS != 2
              .collect(Collectors.toList());
      if (CollectionUtils.isNotEmpty(activeBatchList)) {
        if (activeBatchList.size() > 1) {
          for (Map<String, Object> batch : activeBatchList) {
            String batchAttributesStr = (String) batch.get(CourseJsonKey.BATCH_ATTRIBUTES);
            Map<String, Object> batchAttributes = new HashMap<>();
            if (StringUtils.isNotBlank(batchAttributesStr)) {
              try {
                batchAttributes = mapper.readValue(batchAttributesStr, new TypeReference<Map<String, Object>>() {});
              } catch (JsonProcessingException e) {
                log.error("Error parsing batch attributes JSON: " + batchAttributesStr, e);
              }
            }
            if (MapUtils.isNotEmpty(batchAttributes)) {
              Boolean isActiveBatch = (Boolean) batchAttributes.get(CourseJsonKey.IS_ACTIVE_BATCH);
              if (isActiveBatch != null && isActiveBatch) {
                log.info("Selected active batch for enrolment is: {} for courseId: {}", batch.get(JsonKey.BATCH_ID), courseId);
                batch.remove(JsonKey.PARTICIPANT);
                return mapper.convertValue(batch, CourseBatch.class);
              }
            }
          }
          log.error("Multiple batches available for enrollment. No batch is marked as active (isActiveBatch=true) for courseId: {}", courseId);
          throw new ProjectCommonException(
                  ResponseCode.courseDoesNotHaveBatch.getErrorCode(),
                  ResponseCode.invalidCourseBatchId.getErrorMessage(),
                  ResponseCode.CLIENT_ERROR.getResponseCode());

        } else {
          Map<String, Object> batch = activeBatchList.get(0);
          batch.remove(JsonKey.PARTICIPANT);
          return mapper.convertValue(batch, CourseBatch.class);
        }
      } else {
        throw new ProjectCommonException(
                ResponseCode.courseDoesNotHaveBatch.getErrorCode(),
                ResponseCode.invalidCourseBatchId.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    }
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
  public CourseBatch readByIdWithLocalQuorum(String courseId, String batchId, RequestContext requestContext) {

    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);

    Response courseBatchResult =
            cassandraOperation.getRecordByIdentifier(
                    requestContext,
                    courseBatchDb.getKeySpace(),
                    courseBatchDb.getTableName(),
                    primaryKey,
                    null,
                    ConsistencyLevel.LOCAL_QUORUM
            );

    List<Map<String, Object>> courseList =
            (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);

    if (CollectionUtils.isEmpty(courseList)) {
      throw new ProjectCommonException(
              ResponseCode.invalidCourseBatchId.getErrorCode(),
              ResponseCode.invalidCourseBatchId.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    courseList.get(0).remove(JsonKey.PARTICIPANT);
    CourseBatchCacheUtil.put(courseList.get(0));
    return mapper.convertValue(courseList.get(0), CourseBatch.class);
  }
}
