/** */
package org.sunbird.learner.util;

import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerUtil;
import org.sunbird.common.models.util.PropertiesCache;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class will handle the data cache.
 *
 * @author Amit Kumar
 */
public class BatchCacheHandler implements Runnable {

  private static Map<String, Object> batchMap = new ConcurrentHashMap<>();

  private LoggerUtil logger = new LoggerUtil(BatchCacheHandler.class);

  @Override
  public void run() {
    logger.info(null, "BatchCacheHandler:run: Cache refresh started.");
    cache(batchMap);
    logger.info(null, "BatchCacheHandler:run: Cache refresh completed.");
  }

  @SuppressWarnings("unchecked")
  private void cache(Map<String, Object> map) {
    try {
      Map contents = ContentUtil.getAllBatches(Integer.parseInt(PropertiesCache.getInstance()
          .getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH)));
      batchMap.putAll(contents);
      logger.debug(null, "content keyset " + map.keySet());
      logger.info(null, " cache size: " + map.size());
    } catch (Exception e) {
      logger.error(null, "BatchCacheHandler:cache: Exception in retrieving content section " + e.getMessage(), e);
    }
  }

  /** @return the contentCache */
  public static Map<String, Object> getBatchMap() {
    return batchMap;
  }

  public static Map<String, Object> getBatch(String id) {
    Map<String, Object> obj = (Map<String, Object>) batchMap.get(id);
    if (obj != null)
      return obj;
    else {
      batchMap.putAll(ContentUtil.getAllBatches(Arrays.asList(id), Integer.parseInt(PropertiesCache.getInstance()
          .getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH))));
      return (Map<String, Object>) batchMap.get(id);
    }
  }
}
