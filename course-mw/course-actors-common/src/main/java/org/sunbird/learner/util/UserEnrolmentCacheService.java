package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
 * Read-through cache for a user's course enrolments, keyed ce:v1:{userId}
 * (HASH, field = courseId, value = JSON array of that course's enrolment rows, one per batch —
 * normally one active and zero or more inactive). courseId as the field makes "is the user
 * enrolled in course X" an O(1) HGET regardless of how many other enrolments the user has.
 *
 * Gated by user_enrolment_cache_enabled. When the flag is off every call goes straight to
 * Cassandra and returns the row exactly as the DAO does today. When it is on, rows are returned
 * in the allow-listed shape whether they came from Redis or Cassandra, so a hit and a miss
 * produce identical API output.
 */
public class UserEnrolmentCacheService {

  private static final String KEY_PREFIX = "ce:v1:";
  /**
   * Marker field: present (= "yes") once the hash holds the user's complete enrolment set. A hash
   * without it is partial (built by single-field writes) and is authoritative for nothing: full reads
   * reload it from Cassandra, scoped reads treat a missing field as "unknown", not "not enrolled".
   * Underscore prefix so readers strip it like any other control field.
   */
  private static final String FULL_SCAN = "_fullScan";

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

  private static final TypeReference<List<Map<String, Object>>> LIST_TYPE = new TypeReference<List<Map<String, Object>>>() {};

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
    if (cached.isDefined() && cached.get().containsKey(FULL_SCAN)) {
      logger.info(requestContext, "UserEnrolmentCacheService: cache hit (Redis) for userId " + userId);
      List<Map<String, Object>> rows = new ArrayList<>();
      for (Map.Entry<String, String> e : cached.get().entrySet()) {
        if (e.getKey().startsWith("_")) continue;
        rows.addAll(fromJsonList(e.getValue()));
      }
      return rows;
    }
    logger.info(requestContext, "UserEnrolmentCacheService: " + (cached.isDefined() ? "partial hash" : "cache miss") + " (Redis), fetching enrolments from Cassandra (DB) for userId " + userId);
    List<Map<String, Object>> dbRows = userCoursesDao.listEnrolments_v2(requestContext, userId, null);
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, List<Map<String, Object>>> byCourse = new HashMap<>();
    if (CollectionUtils.isNotEmpty(dbRows)) {
      for (Map<String, Object> dbRow : dbRows) {
        Map<String, Object> row = project(dbRow);
        rows.add(row);
        byCourse.computeIfAbsent((String) row.get(JsonKey.COURSE_ID), k -> new ArrayList<>()).add(row);
      }
    }
    Map<String, String> hash = new HashMap<>();
    for (Map.Entry<String, List<Map<String, Object>>> e : byCourse.entrySet()) {
      String json = toJsonList(e.getValue());
      if (json != null) hash.put(e.getKey(), json);
    }
    hash.put(FULL_SCAN, "yes");
    UserEnrolmentRedisClient.hsetAll(key, hash);
    return rows;
  }

  /** True when the user's hash exists and is complete. */
  private boolean isFullyCached(String userId) {
    return UserEnrolmentRedisClient.hget(cacheKey(userId), FULL_SCAN).isDefined();
  }

  /**
   * Enrolment rows for the given course ids only (all batches of each). O(1) per course: one HMGET on
   * a complete hash; on a partial/absent hash a missing field means "unknown", so that course is read from
   * Cassandra by (userid, courseid) and the answer is written into the hash. Flag off: Cassandra per course.
   */
  public List<Map<String, Object>> getCourseEnrolments(RequestContext requestContext, String userId, List<String> courseIds) {
    if (CollectionUtils.isEmpty(courseIds)) return getCourseEnrolments(requestContext, userId);
    List<String> ids = new ArrayList<>();
    for (String id : courseIds) if (StringUtils.isNotBlank(id) && !ids.contains(id)) ids.add(id);
    List<Map<String, Object>> rows = new ArrayList<>();
    if (!isEnabled()) {
      for (String courseId : ids) {
        List<Map<String, Object>> dbRows = userCoursesDao.listEnrolments_v2(requestContext, userId, Collections.singletonList(courseId));
        if (CollectionUtils.isNotEmpty(dbRows)) rows.addAll(dbRows);
      }
      return rows;
    }
    String key = cacheKey(userId);
    List<String> fields = new ArrayList<>(ids);
    fields.add(FULL_SCAN);
    scala.Option<Map<String, String>> got = UserEnrolmentRedisClient.hmget(key, fields);
    Map<String, String> present = got.isDefined() ? got.get() : Collections.emptyMap();
    boolean full = present.containsKey(FULL_SCAN);
    for (String courseId : ids) {
      String json = present.get(courseId);
      if (json != null) {                       // a present field is always a real answer
        rows.addAll(fromJsonList(json));
      } else if (!full) {                       // partial/absent hash: ask Cassandra and remember
        rows.addAll(loadCourseIntoCache(requestContext, userId, courseId));
      }                                         // full hash + absent field: not enrolled
    }
    return rows;
  }

  /** Cassandra (userid, courseid) -> project -> HSET courseId = batches. Returns the projected rows. */
  private List<Map<String, Object>> loadCourseIntoCache(RequestContext requestContext, String userId, String courseId) {
    List<Map<String, Object>> dbRows = userCoursesDao.listEnrolments_v2(requestContext, userId, Collections.singletonList(courseId));
    List<Map<String, Object>> rows = new ArrayList<>();
    if (CollectionUtils.isEmpty(dbRows)) return rows;
    for (Map<String, Object> dbRow : dbRows) rows.add(project(dbRow));
    String json = toJsonList(rows);
    if (json != null) UserEnrolmentRedisClient.hset(cacheKey(userId), courseId, json);
    return rows;
  }

  /** One enrolment by (courseId, batchId): HGET courseId and pick the batch (flag on) -> Cassandra. Null when absent. */
  public Map<String, Object> getEnrolment(RequestContext requestContext, String userId, String courseId, String batchId) {
    if (isEnabled()) {
      scala.Option<String> json = UserEnrolmentRedisClient.hget(cacheKey(userId), courseId);
      List<Map<String, Object>> rows = json.isDefined() ? fromJsonList(json.get()) : loadCourseIntoCache(requestContext, userId, courseId);
      for (Map<String, Object> row : rows) if (batchId.equals(row.get(JsonKey.BATCH_ID))) return row;
      return null;
    }
    List<Map<String, Object>> dbRows = userCoursesDao.getEnrolmentByBatchIdAndCourseId_v2(requestContext, userId, courseId, batchId);
    return CollectionUtils.isEmpty(dbRows) ? null : dbRows.get(0);
  }

  /**
   * After any write to (userId, courseId, batchId): re-read ALL of the course's rows from Cassandra and
   * upsert the courseId field (HDEL when none remain). Called by course-service APIs; Flink completion /
   * certificate jobs use the same contract. The hash may be absent or partial — the write creates/extends
   * it without the _fullScan marker and the next full read completes it. Reading back keeps the cached
   * value identical to what a miss would produce; on any failure the key is dropped.
   */
  public void refreshEnrolment(RequestContext requestContext, String userId, String courseId, String batchId) {
    if (!isEnabled()) return;
    String key = cacheKey(userId);
    try {
      List<Map<String, Object>> dbRows = userCoursesDao.listEnrolments_v2(requestContext, userId, Collections.singletonList(courseId));
      if (CollectionUtils.isEmpty(dbRows)) {
        UserEnrolmentRedisClient.hdel(key, courseId);
        return;
      }
      List<Map<String, Object>> rows = new ArrayList<>();
      for (Map<String, Object> dbRow : dbRows) rows.add(project(dbRow));
      String json = toJsonList(rows);
      if (json == null || !UserEnrolmentRedisClient.hset(key, courseId, json)) {
        UserEnrolmentRedisClient.del(key);
      }
    } catch (Exception e) {
      logger.error(requestContext, "UserEnrolmentCacheService: refresh failed for " + userId + "/" + courseId + "/" + batchId, e);
      UserEnrolmentRedisClient.del(key);
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

  private String toJsonList(List<Map<String, Object>> rows) {
    try {
      List<Map<String, Object>> out = new ArrayList<>(rows.size());
      for (Map<String, Object> row : rows) {
        Map<String, Object> m = new HashMap<>(row);
        for (String d : DATE_FIELDS) {
          Object v = m.get(d);
          if (v instanceof Date) m.put(d, ((Date) v).getTime());
        }
        out.add(m);
      }
      return mapper.writeValueAsString(out);
    } catch (Exception e) {
      logger.error(null, "UserEnrolmentCacheService: serialise failed", e);
      return null;
    }
  }

  private List<Map<String, Object>> fromJsonList(String json) {
    try {
      List<Map<String, Object>> rows = mapper.readValue(json, LIST_TYPE);
      for (Map<String, Object> row : rows) {
        for (String d : DATE_FIELDS) {
          Object v = row.get(d);
          if (v instanceof Number) row.put(d, new Date(((Number) v).longValue()));
        }
      }
      return rows;
    } catch (Exception e) {
      logger.error(null, "UserEnrolmentCacheService: deserialise failed", e);
      return new ArrayList<>();
    }
  }
}
