package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.sunbird.cache.util.UserEnrolmentRedisClient;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.request.RequestContext;
import org.sunbird.learner.actors.coursebatch.dao.UserCoursesDao;
import org.sunbird.learner.actors.coursebatch.dao.impl.UserCoursesDaoImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Read-through cache for a user's course enrolments, keyed course-enrolments:v1:{userId}
 * (HASH, field = {courseId}:{batchId}, value = allow-listed row JSON).
 *
 * Gated by user_enrolment_cache_enabled. When the flag is off every call goes straight to
 * Cassandra and returns the row exactly as the DAO does today. When it is on, rows are returned
 * in the allow-listed shape whether they came from Redis or Cassandra, so a hit and a miss
 * produce identical API output.
 */
public class UserEnrolmentCacheService {

  private static final String KEY_PREFIX = "course-enrolments:v1:";
  private static final String EMPTY_MARKER = "_empty";

  /** Fields written to Redis. Keys are the DAO map keys (camelCase per cassandratablecolumn.properties). */
  static final Set<String> ALLOWED_FIELDS = new LinkedHashSet<>(Arrays.asList(
      JsonKey.COURSE_ID, JsonKey.BATCH_ID, JsonKey.ACTIVE, JsonKey.STATUS, JsonKey.PROGRESS,
      "enrolledDate", JsonKey.COMPLETED_ON, "lastContentAccessTime", JsonKey.RECENT_LANGUAGE,
      JsonKey.ISSUED_CERTIFICATES));
  private static final Set<String> DATE_FIELDS = new LinkedHashSet<>(Arrays.asList(
      "enrolledDate", JsonKey.COMPLETED_ON, "lastContentAccessTime"));
  private static final Set<String> CERTIFICATE_FIELDS = new LinkedHashSet<>(Arrays.asList(
      JsonKey.IDENTIFIER, JsonKey.NAME, "lastIssuedOn"));

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};
  private static UserEnrolmentCacheService instance;

  private final LoggerUtil logger = new LoggerUtil(UserEnrolmentCacheService.class);
  private final UserCoursesDao userCoursesDao = new UserCoursesDaoImpl();

  private UserEnrolmentCacheService() {}

  public static UserEnrolmentCacheService getInstance() {
    if (instance == null) {
      synchronized (UserEnrolmentCacheService.class) {
        if (instance == null) instance = new UserEnrolmentCacheService();
      }
    }
    return instance;
  }

  public static boolean isEnabled() {
    return UserEnrolmentRedisClient.enabled();
  }

  public static String cacheKey(String userId) {
    return KEY_PREFIX + userId;
  }

  public static String field(String courseId, String batchId) {
    return courseId + ":" + batchId;
  }

  /**
   * All enrolment rows for the user (active and inactive). Never returns null.
   * Flag off: Cassandra, raw rows. Flag on: Redis, Cassandra on miss (then cached), allow-listed rows.
   */
  public List<Map<String, Object>> getCourseEnrolments(RequestContext requestContext, String userId) {
    if (!isEnabled()) {
      logger.info(requestContext, "UserEnrolmentCacheService: cache disabled, fetching enrolments from Cassandra (DB) for userId " + userId);
      List<Map<String, Object>> rows = userCoursesDao.listEnrolments_v2(requestContext, userId, null);
      return rows == null ? new ArrayList<>() : rows;
    }
    String key = cacheKey(userId);
    scala.Option<Map<String, String>> cached = UserEnrolmentRedisClient.hgetAll(key);
    if (cached.isDefined()) {
      logger.info(requestContext, "UserEnrolmentCacheService: cache hit (Redis) for userId " + userId);
      List<Map<String, Object>> rows = new ArrayList<>();
      for (Map.Entry<String, String> e : cached.get().entrySet()) {
        if (e.getKey().startsWith("_")) continue;
        Map<String, Object> row = fromJson(e.getValue());
        if (row != null) rows.add(row);
      }
      return rows;
    }
    logger.info(requestContext, "UserEnrolmentCacheService: cache miss (Redis), fetching enrolments from Cassandra (DB) for userId " + userId);
    List<Map<String, Object>> dbRows = userCoursesDao.listEnrolments_v2(requestContext, userId, null);
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, String> hash = new HashMap<>();
    if (CollectionUtils.isNotEmpty(dbRows)) {
      for (Map<String, Object> dbRow : dbRows) {
        Map<String, Object> row = project(dbRow);
        rows.add(row);
        String json = toJson(row);
        if (json != null) {
          hash.put(field((String) row.get(JsonKey.COURSE_ID), (String) row.get(JsonKey.BATCH_ID)), json);
        }
      }
    }
    if (hash.isEmpty()) hash.put(EMPTY_MARKER, "1");
    UserEnrolmentRedisClient.hsetAll(key, hash);
    return rows;
  }

  /**
   * Re-read one enrolment from Cassandra and write it into the user's hash, if the user is cached.
   * Called after every enrolment write (course-service APIs; Flink completion/certificate jobs use
   * the same contract). Reading back rather than trusting the caller's partial update map keeps the
   * cached value identical to what a miss would produce.
   */
  public void refreshEnrolment(RequestContext requestContext, String userId, String courseId, String batchId) {
    if (!isEnabled()) return;
    try {
      String key = cacheKey(userId);
      String f = field(courseId, batchId);
      List<Map<String, Object>> rows =
          userCoursesDao.getEnrolmentByBatchIdAndCourseId_v2(requestContext, userId, courseId, batchId);
      if (CollectionUtils.isEmpty(rows)) {
        UserEnrolmentRedisClient.hdel(key, f);
        return;
      }
      String json = toJson(project(rows.get(0)));
      if (json == null || !UserEnrolmentRedisClient.hsetIfExists(key, f, json)) {
        // hash absent (user not cached) is fine; a failed write must not leave a stale field
        if (json == null) UserEnrolmentRedisClient.del(key);
        return;
      }
      // a newly cached user may still carry the empty marker
      UserEnrolmentRedisClient.hdel(key, EMPTY_MARKER);
    } catch (Exception e) {
      logger.error(requestContext, "UserEnrolmentCacheService: refresh failed for " + userId + "/" + courseId + "/" + batchId, e);
      UserEnrolmentRedisClient.del(cacheKey(userId));
    }
  }

  public void evict(String userId) {
    if (!isEnabled()) return;
    UserEnrolmentRedisClient.del(cacheKey(userId));
  }

  // ---- projection ----

  @SuppressWarnings("unchecked")
  static Map<String, Object> project(Map<String, Object> dbRow) {
    Map<String, Object> row = new HashMap<>();
    for (String k : ALLOWED_FIELDS) {
      Object v = dbRow.get(k);
      if (v == null) continue;
      if (JsonKey.ISSUED_CERTIFICATES.equals(k) && v instanceof List) {
        List<Map<String, Object>> trimmed = new ArrayList<>();
        for (Object c : (List<Object>) v) {
          if (!(c instanceof Map)) continue;
          Map<String, Object> cert = new HashMap<>();
          for (String cf : CERTIFICATE_FIELDS) {
            Object cv = ((Map<String, Object>) c).get(cf);
            if (cv != null) cert.put(cf, cv);
          }
          trimmed.add(cert);
        }
        row.put(k, trimmed);
      } else {
        row.put(k, v);
      }
    }
    return row;
  }

  private String toJson(Map<String, Object> row) {
    try {
      Map<String, Object> out = new HashMap<>(row);
      for (String d : DATE_FIELDS) {
        Object v = out.get(d);
        if (v instanceof Date) out.put(d, ((Date) v).getTime());
      }
      return mapper.writeValueAsString(out);
    } catch (Exception e) {
      logger.error(null, "UserEnrolmentCacheService: serialise failed", e);
      return null;
    }
  }

  private Map<String, Object> fromJson(String json) {
    try {
      Map<String, Object> row = mapper.readValue(json, MAP_TYPE);
      for (String d : DATE_FIELDS) {
        Object v = row.get(d);
        if (v instanceof Number) row.put(d, new Date(((Number) v).longValue()));
      }
      return row;
    } catch (Exception e) {
      logger.error(null, "UserEnrolmentCacheService: deserialise failed", e);
      return null;
    }
  }
}
