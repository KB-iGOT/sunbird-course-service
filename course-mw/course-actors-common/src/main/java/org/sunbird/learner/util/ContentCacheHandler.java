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
public class ContentCacheHandler implements Runnable {

  private static Map<String, Object> contentMap = new ConcurrentHashMap<>();

  private LoggerUtil logger = new LoggerUtil(ContentCacheHandler.class);
  
  @Override
  public void run() {
    logger.info(null, "ContentCacheHandler:run: Cache refresh started.");
    cache(contentMap);
    logger.info(null, "ContentCacheHandler:run: Cache refresh completed.");
  }

  @SuppressWarnings("unchecked")
  private void cache(Map<String, Object> map) {
    try {
     Map contents =  ContentUtil.getAllContent(Integer.parseInt(PropertiesCache.getInstance()
             .getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH)));
     Map externalContents =  ContentUtil.getAllExternalContent(Integer.parseInt(PropertiesCache.getInstance()
                .getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH)));
      contentMap.putAll(contents);
      contentMap.putAll(externalContents);
          logger.debug(null, "content keyset " + map.keySet());
      logger.info(null,  " cache size: " + map.size());
    } catch (Exception e) {
      logger.error(null, "ContentCacheHandler:cache: Exception in retrieving content section " + e.getMessage(), e);
    }
  }

  /** @return the contentCache */
  public static Map<String, Object> getContentMap() {
    return contentMap;
  }

  public static Map<String, Object> getContent(String id) {
      Map<String, Object> obj = (Map<String, Object>)contentMap.get(id);
    if(obj != null)
       return obj;
    else{
        contentMap.putAll(ContentUtil.getAllContent(Arrays.asList(id),Integer.parseInt(PropertiesCache.getInstance()
                .getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH))));
       return (Map<String, Object>)contentMap.get(id);
    }
  }

    public static Map<String, Object> getExternalContent(String id) {
        Map<String, Object> obj = (Map<String, Object>)contentMap.get(id);
        if(obj != null)
            return obj;
        else{
            contentMap.putAll(ContentUtil.getAllExternalContent(Arrays.asList(id),Integer.parseInt(PropertiesCache.getInstance()
                    .getProperty(JsonKey.PAGE_SIZE_CONTENT_FETCH))));
            return (Map<String, Object>)contentMap.get(id);
        }
    }
}
