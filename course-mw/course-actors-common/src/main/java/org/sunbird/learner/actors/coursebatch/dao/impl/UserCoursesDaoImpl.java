package org.sunbird.learner.actors.coursebatch.dao.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.logstash.logback.encoder.org.apache.commons.lang3.StringUtils;

import java.util.*;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Constants;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.RequestContext;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.dao.UserCoursesDao;
import org.sunbird.learner.util.ExtendedUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.courses.UserCourses;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.util.PropertiesCache;

public class UserCoursesDaoImpl implements UserCoursesDao {
  protected LoggerUtil logger = new LoggerUtil(this.getClass()); 
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  static UserCoursesDao userCoursesDao;
  private static final String KEYSPACE_NAME =
      Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB).getKeySpace();
  private static final String TABLE_NAME =
      Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB).getTableName();
  private static final String USER_ENROLMENTS = Util.dbInfoMap.get(JsonKey.USER_ENROLMENTS_DB).getTableName();
  private static final String ENROLMENT_BATCH_LOOKUP = Util.dbInfoMap.get(JsonKey.ENROLLMENT_BATCH_DB).getTableName();
  private static final String USER_ENROLMENTS_V2 = ExtendedUtil.dbInfoMap.get(JsonKey.USER_ENROLMENTS_V2_DB).getTableName();
  private static final String EXTERNAL_TRAINING_ENROLMENT_BATCH_LOOKUP = ExtendedUtil.dbInfoMap.get(JsonKey.EXTERNAL_TRAINING_ENROLLMENT_BATCH_DB).getTableName();
  public static UserCoursesDao getInstance() {
    if (userCoursesDao == null) {
      userCoursesDao = new UserCoursesDaoImpl();
    }
    return userCoursesDao;
  }
  private RedisCacheUtil redisCacheUtil = new RedisCacheUtil();

  @Override
  public UserCourses read(RequestContext requestContext, String batchId, String userId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    primaryKey.put(JsonKey.USER_ID, userId);
    Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, TABLE_NAME, primaryKey, null);
    List<Map<String, Object>> userCoursesList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    try {
      return mapper.convertValue((Map<String, Object>) userCoursesList.get(0), UserCourses.class);
    } catch (Exception e) {
      logger.error(requestContext, "Failed to read user enrollment table. Exception: ", e);
    }
    return null;
  }


  @Override
  public Response update(RequestContext requestContext, String batchId, String userId, Map<String, Object> updateAttributes) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    primaryKey.put(JsonKey.USER_ID, userId);
    Map<String, Object> updateList = new HashMap<>();
    updateList.putAll(updateAttributes);
    updateList.remove(JsonKey.BATCH_ID);
    updateList.remove(JsonKey.USER_ID);
    return cassandraOperation.updateRecord(requestContext, KEYSPACE_NAME, TABLE_NAME, updateList, primaryKey);
  }

  @Override
  public List<String> getAllActiveUserOfBatch(RequestContext requestContext, String batchId) {
    return getBatchParticipants(requestContext, batchId, true);
  }

  @Override
  public Response batchInsert(RequestContext requestContext, List<Map<String, Object>> userCoursesDetails) {
    return cassandraOperation.batchInsert(requestContext, KEYSPACE_NAME, USER_ENROLMENTS, userCoursesDetails);
  }

  @Override
  public Response insert(RequestContext requestContext, Map<String, Object> userCoursesDetails) {
    return cassandraOperation.insertRecord(requestContext, KEYSPACE_NAME, TABLE_NAME, userCoursesDetails);
  }

  @Override
  public Response insertV2(RequestContext requestContext, Map<String, Object> userCoursesDetails) {
    return cassandraOperation.insertRecord(requestContext, KEYSPACE_NAME, USER_ENROLMENTS, userCoursesDetails);
  }

  @Override
  public Response updateV2(RequestContext requestContext, String userId, String courseId, String batchId, Map<String, Object> updateAttributes) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    Map<String, Object> updateList = new HashMap<>();
    updateList.putAll(updateAttributes);
    updateList.remove(JsonKey.BATCH_ID_KEY);
    updateList.remove(JsonKey.COURSE_ID_KEY);
    updateList.remove(JsonKey.USER_ID_KEY);
    return cassandraOperation.updateRecord(requestContext, KEYSPACE_NAME, USER_ENROLMENTS, updateList, primaryKey);
  }

  @Override
  public UserCourses read(RequestContext requestContext, String userId, String courseId, String batchId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, USER_ENROLMENTS_V2, primaryKey, null);
    List<Map<String, Object>> userCoursesList =
            (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    try {
      return mapper.convertValue((Map<String, Object>) userCoursesList.get(0), UserCourses.class);
    } catch (Exception e) {
      logger.error(requestContext, "Failed to read user enrollments table. Exception: ", e);
    }
    return null;
  }

  @Override
  public List<String> getBatchParticipants(RequestContext requestContext, String batchId, boolean active) {
    Map<String, Object> queryMap = new HashMap<>();
    queryMap.put(JsonKey.BATCH_ID, batchId);
    Response response =
            cassandraOperation.getRecordByIdentifier(requestContext,KEYSPACE_NAME, ENROLMENT_BATCH_LOOKUP, queryMap, null);
        /*cassandraOperation.getRecords(
                requestContext, KEYSPACE_NAME, USER_ENROLMENTS, queryMap, Arrays.asList(JsonKey.USER_ID, JsonKey.ACTIVE));*/
    List<Map<String, Object>> userCoursesList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    List<String> userList = new ArrayList<String>();
    for(Map<String, Object> userCourse : userCoursesList) {
      if(userCourse.get(JsonKey.ACTIVE) != null 
        && (active == (boolean) userCourse.get(JsonKey.ACTIVE))) {
        userList.add((String) userCourse.get(JsonKey.USER_ID));
      }
    }
    return userList;
  }

  private List<Map<String, Object>> listEnrolmentsInternal(RequestContext requestContext, String userId, List<String> courseIdList, String tableName) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);

    List<Map<String, Object>> userCoursesList;

    if (CollectionUtils.isNotEmpty(courseIdList) && courseIdList.size() == 1 && StringUtils.isNotBlank(courseIdList.get(0))) {
      primaryKey.put(JsonKey.COURSE_ID_KEY, courseIdList);
      Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, tableName, primaryKey, null);
      userCoursesList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    } else {
      Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, tableName, primaryKey, null);
      userCoursesList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (courseIdList != null && !courseIdList.isEmpty() && userCoursesList != null) {
        List<Map<String, Object>> filteredList = new ArrayList<>();
        for (Map<String, Object> enrolment : userCoursesList) {
          String enrolmentCourseId = (String) enrolment.get(JsonKey.COURSE_ID);
          if (courseIdList.contains(enrolmentCourseId)) {
            filteredList.add(enrolment);
          }
        }
        userCoursesList = filteredList;
      }
    }

    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    return userCoursesList;
  }

  @Override
  public Map<String, Object> getBatchParticipantsByPage(RequestContext requestContext, Map<String, Object> request) {
    logger.info(requestContext, "UserCourseDao:: getBatchParticipantsByPage:: Received request:: " + request);
    Map<String, Object> queryMap = new HashMap<>();
    queryMap.put(JsonKey.BATCH_ID, (String) request.get(JsonKey.BATCH_ID));
    Map<String, Object> result = new HashMap<String, Object>();
    List<String> userList = new ArrayList<String>();
    Boolean active = (Boolean) request.get(JsonKey.ACTIVE);
    if (null == active) {
      active = true;
    }
    Integer limit = (Integer) request.get(JsonKey.LIMIT);
    if (limit == null) {
      limit = Constants.DEFAULT_LIMIT;
    }
    Integer currentOffSetFromRequest = (Integer) request.get(JsonKey.CURRENT_OFFSET);
    if (currentOffSetFromRequest == null) {
      currentOffSetFromRequest = 0;
    }
    String pageId = (String) request.get(JsonKey.PAGE_ID);
    String previousPageId = null;
    int currentOffSet = 1;
    String currentPagingState = null;
    long count = 0L;
    long activeCount = 0L;
    Response res = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            requestContext,
            KEYSPACE_NAME,
            ENROLMENT_BATCH_LOOKUP,
            JsonKey.BATCH_ID,
            request.get(JsonKey.BATCH_ID),
            Arrays.asList(JsonKey.USER_ID, JsonKey.ACTIVE)
    );
    List<Map<String, Object>> batchUsers =
            (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);

    if (CollectionUtils.isNotEmpty(batchUsers)) {
        activeCount = batchUsers.stream()
                .filter(row -> Boolean.TRUE.equals(row.get(JsonKey.ACTIVE)))
                .count();
    }
    logger.info(requestContext, "Total enrolment in the batch : " + (String) request.get(JsonKey.BATCH_ID) + " is: " + count);
    do {
      Response response = cassandraOperation.getRecordByIdentifierWithPage(requestContext, KEYSPACE_NAME,
          ENROLMENT_BATCH_LOOKUP, queryMap,
          null, pageId, (Integer) request.get(JsonKey.LIMIT));
      currentPagingState = (String) response.get(JsonKey.PAGE_ID);
      if (StringUtils.isBlank(previousPageId) && StringUtils.isNotBlank(currentPagingState)) {
        previousPageId = currentPagingState;
      }
      pageId = currentPagingState;
      List<Map<String, Object>> userCoursesList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (CollectionUtils.isEmpty(userCoursesList)) {
        //Set null so that client knows there are no data to read further.
        previousPageId = null;
        break;
      }
      for (Map<String, Object> userCourse : userCoursesList) {
        //From this page, we have already read some records, so skip the records
        if (currentOffSetFromRequest > 0) {
          currentOffSetFromRequest--;
          continue;
        }
        if (userCourse.get(JsonKey.ACTIVE) != null
            && (active == (boolean) userCourse.get(JsonKey.ACTIVE))) {
          userList.add((String) userCourse.get(JsonKey.USER_ID));
          if (userList.size() == limit) {
            //We have read the data... if pageId available send back in response.
            previousPageId = pageId != null ? pageId : previousPageId;
            break;
          }
        }
        currentOffSet++;
      }
      //We may have read the given limit... if so, break from while loop
      if (userList.size() == limit) {
        break;
      }
    } while (StringUtils.isNotBlank(currentPagingState));
    if (StringUtils.isNotBlank(previousPageId)) {
      result.put(JsonKey.PAGE_ID, previousPageId);
      if (currentOffSet >= limit) {
        currentOffSet = currentOffSet - limit;
      }
    }
    result.put(JsonKey.CURRENT_OFFSET, (Integer) request.get(JsonKey.CURRENT_OFFSET));
    //Only active users will be returned.
    result.put(JsonKey.COUNT, activeCount);
    result.put(JsonKey.PARTICIPANTS, userList);
    return result;
  }

  public List<UserCourses> readAll(RequestContext requestContext, String userId, String courseId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, USER_ENROLMENTS, primaryKey, null);
    List<Map<String, Object>> userCoursesList =
            (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    try {
      List<UserCourses> convertedUserCoursesList = new ArrayList<>();
      for (Object userCourseObj : userCoursesList) {
        if (userCourseObj instanceof Map) {
          Map<String, Object> userCourseMap = (Map<String, Object>) userCourseObj;
          convertedUserCoursesList.add(mapper.convertValue(userCourseMap, UserCourses.class));
        }
      }
      return convertedUserCoursesList;
    } catch (Exception e) {
      logger.error(requestContext, "Failed to read user enrollments table. Exception: ", e);
    }
    return null;
  }

  private List<Map<String, Object>> getEnrolmentByBatchIdAndCourseIdInternal(
          RequestContext requestContext, String userId, String courseId, String batchId, String tableName) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, tableName, primaryKey, null);
    List<Map<String, Object>> userCoursesList =
            (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    return userCoursesList;
  }

  @Override
  public List<UserCourses> readV2(RequestContext requestContext, String userId, String courseId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);
    primaryKey.put(JsonKey.COURSE_ID, courseId);

    Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, USER_ENROLMENTS, primaryKey, null);
    List<Map<String, Object>> userCoursesList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    try {
      return mapper.convertValue(userCoursesList, new TypeReference<List<UserCourses>>() {
      });
    } catch (Exception e) {
      logger.error(requestContext, "Failed to read user enrollments table. Exception: ", e);
    }
    return null;
  }

  public long getCountOfActiveParticipants(RequestContext requestContext, String batchId) {
    String cacheKey = getCacheKey(batchId);
    int ttl = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.PARTICIPANTS_TTL));
    String cachedCount = redisCacheUtil.get(cacheKey, null, ttl);
    if (StringUtils.isNotBlank(cachedCount)) {
      return Long.parseLong(cachedCount);
    }
    int fetchSize = Integer.parseInt(PropertiesCache.getInstance().getProperty(JsonKey.PARTICIPANTS_FETCH_SIZE));
    List<String> activeUsers = new ArrayList<>();
    String pageState = null;
    do {
      Map<String, Object> query = Map.of(JsonKey.BATCH_ID, batchId);
      Response response = cassandraOperation.getRecordByIdentifierWithPage(
              requestContext, KEYSPACE_NAME, ENROLMENT_BATCH_LOOKUP, query, null, pageState, fetchSize
      );

      List<Map<String, Object>> userCourses = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);

      if (CollectionUtils.isNotEmpty(userCourses)) {
        for (Map<String, Object> userCourse : userCourses) {
          if (Boolean.TRUE.equals(userCourse.get(JsonKey.ACTIVE))) {
            activeUsers.add((String) userCourse.get(JsonKey.USER_ID));
          }
        }
      }
      pageState = (String) response.getResult().get(JsonKey.PAGE_ID);
    } while (pageState != null);
    redisCacheUtil.set(cacheKey, String.valueOf(activeUsers.size()), ttl);
    return activeUsers.size();
  }

  private String getCacheKey(String batchId) {
    return batchId + ":active-participants-count";
  }

  public Response insertExtendedEnrollmentV2(RequestContext requestContext, Map<String, Object> data) {
    return cassandraOperation.insertRecord(requestContext, KEYSPACE_NAME, USER_ENROLMENTS_V2, data);
  }

  @Override
  public Response updateExtendedEnrollV2(RequestContext requestContext, String userId, String courseId, String batchId, Map<String, Object> updateAttributes) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    primaryKey.put(JsonKey.BATCH_ID, batchId);
    Map<String, Object> updateList = new HashMap<>();
    updateList.putAll(updateAttributes);
    updateList.remove(JsonKey.BATCH_ID_KEY);
    updateList.remove(JsonKey.COURSE_ID_KEY);
    updateList.remove(JsonKey.USER_ID_KEY);
    return cassandraOperation.updateRecord(requestContext, KEYSPACE_NAME, USER_ENROLMENTS_V2, updateList, primaryKey);
  }

  @Override
  public List<UserCourses> extendedReadV2(RequestContext requestContext, String userId, String courseId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);
    primaryKey.put(JsonKey.COURSE_ID, courseId);

    Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, USER_ENROLMENTS_V2, primaryKey, null);
    List<Map<String, Object>> userCoursesList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    try {
      return mapper.convertValue(userCoursesList, new TypeReference<List<UserCourses>>() {
      });
    } catch (Exception e) {
      logger.error(requestContext, "Failed to read user enrollments table. Exception: ", e);
    }
    return null;
  }

  @Override
  public List<Map<String, Object>> listEnrolments(RequestContext requestContext, String userId, List<String> courseIdList) {
    return listEnrolmentsInternal(requestContext, userId, courseIdList, USER_ENROLMENTS);
  }

  @Override
  public List<Map<String, Object>> listEnrolments_v2(RequestContext requestContext, String userId, List<String> courseIdList) {
    return listEnrolmentsInternal(requestContext, userId, courseIdList, USER_ENROLMENTS_V2);
  }

  @Override
  public List<Map<String, Object>> getEnrolmentByBatchIdAndCourseId(
          RequestContext requestContext, String userId, String courseId, String batchId) {
    return getEnrolmentByBatchIdAndCourseIdInternal(requestContext, userId, courseId, batchId, USER_ENROLMENTS);
  }

  @Override
  public List<Map<String, Object>> getEnrolmentByBatchIdAndCourseId_v2(
          RequestContext requestContext, String userId, String courseId, String batchId) {
    return getEnrolmentByBatchIdAndCourseIdInternal(requestContext, userId, courseId, batchId, USER_ENROLMENTS_V2);
  }

  public List<UserCourses> extendedReadAllV2(RequestContext requestContext, String userId, String courseId) {
    Map<String, Object> primaryKey = new HashMap<>();
    primaryKey.put(JsonKey.USER_ID, userId);
    primaryKey.put(JsonKey.COURSE_ID, courseId);
    Response response = cassandraOperation.getRecordByIdentifier(requestContext, KEYSPACE_NAME, USER_ENROLMENTS_V2, primaryKey, null);
    List<Map<String, Object>> userCoursesList =
            (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(userCoursesList)) {
      return null;
    }
    try {
      List<UserCourses> convertedUserCoursesList = new ArrayList<>();
      for (Object userCourseObj : userCoursesList) {
        if (userCourseObj instanceof Map) {
          Map<String, Object> userCourseMap = (Map<String, Object>) userCourseObj;
          convertedUserCoursesList.add(mapper.convertValue(userCourseMap, UserCourses.class));
        }
      }
      return convertedUserCoursesList;
    } catch (Exception e) {
      logger.error(requestContext, "Failed to read user enrollments table. Exception: ", e);
    }
    return null;
  }

  @Override
  public Map<String, Object> getEventParticipantsForExternalTraining(RequestContext requestContext, Map<String, Object> request) {
    logger.info(requestContext, "UserCourseDao:: getEventParticipantsForExternalTraining:: Received request:: " + request);
    boolean active = Boolean.TRUE.equals(request.getOrDefault(JsonKey.ACTIVE, true));
    int limit = request.get(JsonKey.LIMIT) != null ? (Integer) request.get(JsonKey.LIMIT) : Constants.DEFAULT_LIMIT;
    int currentOffSet = request.get(JsonKey.CURRENT_OFFSET) != null ? (Integer) request.get(JsonKey.CURRENT_OFFSET) : 0;

    long activeCount = fetchExternalTrainingActiveCount(requestContext, request);
    Map<String, Object> pagedResult = collectExternalTrainingPagedUsers(requestContext, request, active, limit, currentOffSet);

    Map<String, Object> result = new HashMap<>();
    if (StringUtils.isNotBlank((String) pagedResult.get(JsonKey.PAGE_ID))) {
      result.put(JsonKey.PAGE_ID, pagedResult.get(JsonKey.PAGE_ID));
    }
    result.put(JsonKey.CURRENT_OFFSET, request.get(JsonKey.CURRENT_OFFSET));
    result.put(JsonKey.COUNT, activeCount);
    result.put(JsonKey.PARTICIPANTS, pagedResult.get(JsonKey.PARTICIPANTS));
    return result;
  }

  private long fetchExternalTrainingActiveCount(RequestContext requestContext, Map<String, Object> request) {
    Response res = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            requestContext,
            KEYSPACE_NAME,
            EXTERNAL_TRAINING_ENROLMENT_BATCH_LOOKUP,
            JsonKey.BATCH_ID,
            request.get(JsonKey.BATCH_ID),
            Arrays.asList(JsonKey.USER_ID, JsonKey.ACTIVE)
    );
    List<Map<String, Object>> batchUsers = (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);
    if (CollectionUtils.isEmpty(batchUsers)) {
      return 0L;
    }
    return batchUsers.stream().filter(row -> Boolean.TRUE.equals(row.get(JsonKey.ACTIVE))).count();
  }

  private Map<String, Object> collectExternalTrainingPagedUsers(RequestContext requestContext, Map<String, Object> request,
                                                                 boolean active, int limit, int offsetFromRequest) {
    Map<String, Object> queryMap = new HashMap<>();
    queryMap.put(JsonKey.BATCH_ID, (String) request.get(JsonKey.BATCH_ID));

    List<String> userList = new ArrayList<>();
    String pageId = (String) request.get(JsonKey.PAGE_ID);
    String previousPageId = null;
    String currentPagingState = null;

    do {
      Response response = cassandraOperation.getRecordByIdentifierWithPage(requestContext, KEYSPACE_NAME,
              EXTERNAL_TRAINING_ENROLMENT_BATCH_LOOKUP, queryMap, null, pageId, limit);
      currentPagingState = (String) response.get(JsonKey.PAGE_ID);
      if (StringUtils.isBlank(previousPageId) && StringUtils.isNotBlank(currentPagingState)) {
        previousPageId = currentPagingState;
      }
      pageId = currentPagingState;

      List<Map<String, Object>> userCoursesList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (CollectionUtils.isEmpty(userCoursesList)) {
        previousPageId = null;
        break;
      }

      offsetFromRequest = processPageUsers(userCoursesList, userList, active, limit, offsetFromRequest);
      if (userList.size() == limit) {
        previousPageId = pageId != null ? pageId : previousPageId;
        break;
      }
    } while (StringUtils.isNotBlank(currentPagingState));

    Map<String, Object> result = new HashMap<>();
    result.put(JsonKey.PAGE_ID, previousPageId);
    result.put(JsonKey.PARTICIPANTS, userList);
    return result;
  }

  private int processPageUsers(List<Map<String, Object>> userCoursesList, List<String> userList,
                                boolean active, int limit, int offsetFromRequest) {
    for (Map<String, Object> userCourse : userCoursesList) {
      if (offsetFromRequest > 0) {
        offsetFromRequest--;
        continue;
      }
      if (Boolean.valueOf(active).equals(userCourse.get(JsonKey.ACTIVE))) {
        userList.add((String) userCourse.get(JsonKey.USER_ID));
        if (userList.size() == limit) {
          break;
        }
      }
    }
    return offsetFromRequest;
  }

  @Override
  public long countActiveParticipants(RequestContext requestContext, String batchId) {
    logger.info(requestContext, "UserCourseDao:: countActiveParticipants:: batchId=" + batchId);
    try {
      Response response = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
          requestContext, KEYSPACE_NAME, ENROLMENT_BATCH_LOOKUP,
          JsonKey.BATCH_ID, batchId,
          Arrays.asList(JsonKey.ACTIVE));
      List<Map<String, Object>> rows = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
      if (CollectionUtils.isEmpty(rows)) {
        logger.info(requestContext, "UserCourseDao:: countActiveParticipants:: No rows found for batchId=" + batchId);
        return 0L;
      }
      long count = rows.stream().filter(row -> Boolean.TRUE.equals(row.get(JsonKey.ACTIVE))).count();
      logger.info(requestContext, "UserCourseDao:: countActiveParticipants:: Active participant count for batchId=" + batchId + " is: " + count);
      return count;
    } catch (Exception e) {
      logger.error(requestContext, "UserCourseDao:: countActiveParticipants:: Failed to count active participants for batchId=" + batchId, e);
      return 0L;
    }
  }

}