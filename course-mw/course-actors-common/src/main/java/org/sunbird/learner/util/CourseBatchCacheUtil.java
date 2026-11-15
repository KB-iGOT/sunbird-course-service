package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Redis cache of course_batch rows on the shared Redis: key {courseId}_{batchId}, value = the raw row as the
 * DAO returns it (camelCase keys, participant removed), TTL course_batch_cache_ttl_seconds.
 *
 * Written by this service on batch create/update (it owns course_batch); read Redis-first by
 * CourseBatchDaoImpl.readById and BatchCacheHandlerV2. Gated by course_batch_cache_enabled; every operation is
 * best-effort so a Redis failure degrades to Cassandra and never fails the caller.
 */
public class CourseBatchCacheUtil {

  private static final LoggerUtil logger = new LoggerUtil(CourseBatchCacheUtil.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};
  private static final RedisCacheUtil redis = new RedisCacheUtil();

  /** Row keys that hold java.util.Date; stored as epoch millis and restored on read. */
  private static final Set<String> DATE_FIELDS = new HashSet<>(Arrays.asList(
      "createdDate", "startDate", "endDate", "updatedDate", "enrollmentEndDate", "enrollment_end_date"));

  private static final boolean ENABLED;
  private static final int INDEX;
  private static final int TTL;

  static {
    PropertiesCache props = PropertiesCache.getInstance();
    ENABLED = "true".equalsIgnoreCase(prop(props, "course_batch_cache_enabled", "false"));
    INDEX = intProp(props, "course_batch_redis_index", 13);
    TTL = intProp(props, "course_batch_cache_ttl_seconds", 86400);
    logger.info(null, "CourseBatchCacheUtil: enabled=" + ENABLED + " index=" + INDEX + " ttl=" + TTL);
  }

  private CourseBatchCacheUtil() {}

  public static boolean isEnabled() { return ENABLED; }

  public static String key(String courseId, String batchId) { return courseId + "_" + batchId; }

  /** Raw row, or null when disabled, absent or on error. */
  public static Map<String, Object> get(String courseId, String batchId) {
    if (!ENABLED || StringUtils.isBlank(courseId) || StringUtils.isBlank(batchId)) return null;
    String k = key(courseId, batchId);
    try {
      String json = redis.getUsingIndex(k, null, 0, INDEX);
      if (StringUtils.isBlank(json)) return null;
      Map<String, Object> row = mapper.readValue(json, MAP_TYPE);
      for (String d : DATE_FIELDS) {
        Object v = row.get(d);
        if (v instanceof Number) row.put(d, new Date(((Number) v).longValue()));
      }
      return row;
    } catch (Exception e) {
      logger.error(null, "CourseBatchCacheUtil:get failed for " + k, e);
      return null;
    }
  }

  /** Store the raw row (as returned by Cassandra, participant removed). */
  public static void put(Map<String, Object> row) {
    if (!ENABLED || row == null) return;
    String courseId = (String) row.get(JsonKey.COURSE_ID);
    String batchId = (String) row.get(JsonKey.BATCH_ID);
    if (StringUtils.isBlank(courseId) || StringUtils.isBlank(batchId)) return;
    String k = key(courseId, batchId);
    try {
      Map<String, Object> out = new HashMap<>(row);
      out.remove(JsonKey.PARTICIPANT);
      for (String d : DATE_FIELDS) {
        Object v = out.get(d);
        if (v instanceof Date) out.put(d, ((Date) v).getTime());
      }
      redis.setUsingIndex(k, mapper.writeValueAsString(out), TTL, INDEX);
    } catch (Exception e) {
      logger.error(null, "CourseBatchCacheUtil:put failed for " + k, e);
    }
  }

  /** Remove from Redis and from the in-memory batch cache on this pod. */
  public static void evict(String courseId, String batchId) {
    if (StringUtils.isNotBlank(batchId)) BatchCacheHandlerV2.getInstance().invalidate(batchId);
    if (!ENABLED || StringUtils.isBlank(courseId) || StringUtils.isBlank(batchId)) return;
    String k = key(courseId, batchId);
    try {
      redis.deleteUsingIndex(k, INDEX);
    } catch (Exception e) {
      logger.error(null, "CourseBatchCacheUtil:evict failed for " + k, e);
    }
  }

  private static String prop(PropertiesCache props, String key, String def) {
    String v = props.getProperty(key);
    return (StringUtils.isBlank(v) || key.equals(v)) ? def : v.trim();
  }

  private static int intProp(PropertiesCache props, String key, int def) {
    try { return Integer.parseInt(prop(props, key, String.valueOf(def))); } catch (NumberFormatException e) { return def; }
  }
}
