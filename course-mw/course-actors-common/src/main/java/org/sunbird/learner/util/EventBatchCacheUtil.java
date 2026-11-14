package org.sunbird.learner.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.helper.ServiceFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * user_event_batches rows: Caffeine (per pod) -> shared Redis "{eventId}_{batchId}" (24 h, written by this
 * service on event-batch create / certificate-template change) -> Cassandra. Same shape as
 * CourseBatchCacheUtil + BatchCacheHandlerV2, folded into one class because events have no separate
 * in-memory handler. Gated by event_batch_cache_enabled; Redis index and TTL shared with course batches.
 * Every Redis operation is best-effort.
 */
public class EventBatchCacheUtil {

  private static final LoggerUtil logger = new LoggerUtil(EventBatchCacheUtil.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};
  private static final RedisCacheUtil redis = new RedisCacheUtil();
  private static final CassandraOperation cassandra = ServiceFactory.getInstance();
  private static final Set<String> DATE_FIELDS = new HashSet<>(Arrays.asList(
      "createdDate", "startDate", "endDate", "updatedDate", "enrollmentEndDate", "enrollment_end_date"));

  private static final boolean ENABLED;
  private static final int INDEX;
  private static final int TTL;
  private static final int MGET_BATCH;
  private static final LoadingCache<String, Map<String, Object>> local;
  private static final Util.DbInfo eventBatchDb = Util.dbInfoMap.get(JsonKey.EVENT_BATCH_DB);

  static {
    PropertiesCache props = PropertiesCache.getInstance();
    ENABLED = "true".equalsIgnoreCase(prop(props, "event_batch_cache_enabled", "false"));
    INDEX = intProp(props, "course_batch_redis_index", 13);
    TTL = intProp(props, "course_batch_cache_ttl_seconds", 86400);
    MGET_BATCH = Math.max(1, intProp(props, "content_cache_mget_batch_size", 50));
    long ttlMinutes = intProp(props, "EVENT_BATCH_CACHE_TTL_MINUTES", 30);
    long maxSize = intProp(props, "EVENT_BATCH_CACHE_MAX_SIZE", 50000);
    local = Caffeine.newBuilder().maximumSize(maxSize)
        .refreshAfterWrite(Duration.ofMinutes(ttlMinutes))
        .recordStats()
        .build(new CacheLoader<String, Map<String, Object>>() {
          @Override public Map<String, Object> load(String key) { return null; } // callers use getIfPresent/put
          @Override public Map<String, Object> reload(String key, Map<String, Object> old) {
            try {
              String json = redis.getUsingIndex(key, null, 0, INDEX);   // Redis is the shared truth; Cassandra only via put()
              return StringUtils.isBlank(json) ? null : fromJson(json); // null = gone from Redis -> entry removed
            } catch (Exception e) {
              logger.error(null, "EventBatchCacheUtil: reload failed for " + key, e);
              return old;                                               // transient failure -> keep serving old
            }
          }
        });
    logger.info(null, "EventBatchCacheUtil: enabled=" + ENABLED + " index=" + INDEX + " ttl=" + TTL);
  }

  private EventBatchCacheUtil() {}

  public static boolean isEnabled() { return ENABLED; }

  public static String key(String eventId, String batchId) { return eventId + "_" + batchId; }

  /** Caffeine -> Redis. Null when disabled, absent or on error (caller falls back to Cassandra). */
  public static Map<String, Object> get(String eventId, String batchId) {
    if (!ENABLED || StringUtils.isBlank(eventId) || StringUtils.isBlank(batchId)) return null;
    String k = key(eventId, batchId);
    Map<String, Object> row = local.getIfPresent(k);
    if (row != null) {
      logger.info(null, "EventBatchCacheUtil:get: local cache (Caffeine) hit for " + k);
      return row;
    }
    try {
      String json = redis.getUsingIndex(k, null, 0, INDEX);
      if (StringUtils.isBlank(json)) return null;
      row = fromJson(json);
      local.put(k, row);
      logger.info(null, "EventBatchCacheUtil:get: cache hit (Redis) for " + k);
      return row;
    } catch (Exception e) {
      logger.error(null, "EventBatchCacheUtil:get failed for " + k, e);
      return null;
    }
  }

  /**
   * Batched: Caffeine getAllPresent, then one Redis MGET per chunk for the misses. Fills Caffeine so the
   * per-row get() calls that follow are in-memory hits. Pairs not found anywhere are simply absent.
   */
  public static Map<String, Map<String, Object>> getAll(Collection<String[]> eventBatchPairs) {
    Map<String, Map<String, Object>> result = new HashMap<>();
    if (!ENABLED || eventBatchPairs == null || eventBatchPairs.isEmpty()) return result;
    Set<String> keys = new LinkedHashSet<>();
    for (String[] p : eventBatchPairs) if (p != null && p.length == 2 && StringUtils.isNotBlank(p[0]) && StringUtils.isNotBlank(p[1])) keys.add(key(p[0], p[1]));
    result.putAll(local.getAllPresent(keys));
    List<String> misses = new ArrayList<>();
    for (String k : keys) if (!result.containsKey(k)) misses.add(k);
    if (misses.isEmpty()) return result;
    for (int from = 0; from < misses.size(); from += MGET_BATCH) {
      List<String> chunk = misses.subList(from, Math.min(from + MGET_BATCH, misses.size()));
      try {
        for (Map.Entry<String, String> e : redis.mgetUsingIndex(chunk, INDEX).entrySet()) {
          Map<String, Object> row = fromJson(e.getValue());
          if (row != null) { local.put(e.getKey(), row); result.put(e.getKey(), row); }
        }
      } catch (Exception e) {
        logger.error(null, "EventBatchCacheUtil:getAll: Redis MGET failed for a chunk of " + chunk.size(), e);
      }
    }
    logger.info(null, "EventBatchCacheUtil:getAll: " + result.size() + " of " + keys.size() + " event batches from cache");
    return result;
  }

  /** Store the raw row (as the DAO returns it, participant removed) in Redis and Caffeine. */
  public static void put(Map<String, Object> row) {
    if (!ENABLED || row == null) return;
    String eventId = (String) (row.get(JsonKey.EVENTID) != null ? row.get(JsonKey.EVENTID) : row.get(JsonKey.EVENT_ID)); // column is "eventid"; no camelCase mapping
    String batchId = (String) row.get(JsonKey.BATCH_ID);
    if (StringUtils.isBlank(eventId) || StringUtils.isBlank(batchId)) return;
    String k = key(eventId, batchId);
    try {
      Map<String, Object> out = new HashMap<>(row);
      out.remove(JsonKey.PARTICIPANT);
      local.put(k, out);
      Map<String, Object> json = new HashMap<>(out);
      for (String d : DATE_FIELDS) { Object v = json.get(d); if (v instanceof Date) json.put(d, ((Date) v).getTime()); }
      redis.setUsingIndex(k, mapper.writeValueAsString(json), TTL, INDEX);
    } catch (Exception e) {
      logger.error(null, "EventBatchCacheUtil:put failed for " + k, e);
    }
  }

  public static void evict(String eventId, String batchId) {
    if (StringUtils.isBlank(eventId) || StringUtils.isBlank(batchId)) return;
    String k = key(eventId, batchId);
    local.invalidate(k);
    if (!ENABLED) return;
    try { redis.deleteUsingIndex(k, INDEX); } catch (Exception e) { logger.error(null, "EventBatchCacheUtil:evict failed for " + k, e); }
  }

  /**
   * Startup pre-warm: for each event id, one Cassandra partition read of user_event_batches (eventid = ?)
   * and put() of every batch found. Called from EventCacheHandlerV2.warmFromTopEvents with the same chunks
   * and pauses as the event metadata, so the shared Redis sees the same pacing.
   */
  public static int warmByEvents(Collection<String> eventIds) {
    if (!ENABLED || eventIds == null || eventIds.isEmpty()) return 0;
    int loaded = 0;
    for (String eventId : eventIds) {
      if (StringUtils.isBlank(eventId)) continue;
      try {
        Map<String, Object> pk = new HashMap<>();
        pk.put(JsonKey.EVENT_ID, eventId);
        Response res = cassandra.getRecordByIdentifier(null, eventBatchDb.getKeySpace(), eventBatchDb.getTableName(), pk, null);
        List<Map<String, Object>> rows = res == null ? null : (List<Map<String, Object>>) res.get(JsonKey.RESPONSE);
        if (CollectionUtils.isNotEmpty(rows)) for (Map<String, Object> row : rows) { put(row); loaded++; }
      } catch (Exception e) {
        logger.error(null, "EventBatchCacheUtil:warmByEvents failed for event " + eventId, e);
      }
    }
    return loaded;
  }

  private static Map<String, Object> fromJson(String json) throws Exception {
    Map<String, Object> row = mapper.readValue(json, MAP_TYPE);
    for (String d : DATE_FIELDS) { Object v = row.get(d); if (v instanceof Number) row.put(d, new Date(((Number) v).longValue())); }
    return row;
  }

  private static String prop(PropertiesCache props, String key, String def) {
    String v = props.getProperty(key);
    return (StringUtils.isBlank(v) || key.equals(v)) ? def : v.trim();
  }

  private static int intProp(PropertiesCache props, String key, int def) {
    try { return Integer.parseInt(prop(props, key, String.valueOf(def))); } catch (NumberFormatException e) { return def; }
  }
}
