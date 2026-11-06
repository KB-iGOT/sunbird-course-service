package org.sunbird.cache.util

import java.time.Duration

import org.apache.commons.lang3.StringUtils
import org.sunbird.cache.platform.Platform
import org.sunbird.common.models.util.{LoggerUtil, PropertiesCache}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * This Utility Object Provide Methods To Perform CRUD Operation With Redis
  */
/**
  * One JedisPool per JVM, shared by every RedisCacheUtil instance. Instances are created in many
  * places (static fields, per-actor Guice injection, per-DAO fields); before this each one owned a
  * 2-connection pool, and the singletons among them (ContentCacheHandlerV2) starved under load.
  */
object RedisCacheUtil {

    private val logger: LoggerUtil = new LoggerUtil(classOf[RedisCacheUtil])

    private val redis_host: String = Platform.getString("sunbird_redis_host", "localhost")
    private val redis_port: Int = Platform.getInteger("sunbird_redis_port", 6379)
    private val index: Int = Platform.getInteger("redis.dbIndex", 0)
    private val redis_timeout: Int = Platform.getInteger("redis.connection.timeout", 10000) // 10 seconds default (used for both connection and socket timeout)
    private val redis_password: String = Platform.getString("sunbird_redis_password", null)

    /** Pool size: redis.connection.max if set, else sunbird_redis_connection_pool_size (externalresource.properties / env), else 64. */
    private val poolMax: Int = {
        val fromPlatform = Platform.getInteger("redis.connection.max", -1)
        if (fromPlatform > 0) fromPlatform
        else {
            val v = PropertiesCache.getInstance.getProperty("sunbird_redis_connection_pool_size")
            try { val n = v.trim.toInt; if (n > 0) n else 64 } catch { case _: Exception => 64 }
        }
    }

    private def buildPoolConfig = {
        val poolConfig = new JedisPoolConfig
        poolConfig.setMaxTotal(poolMax)
        poolConfig.setMaxIdle(Platform.getInteger("redis.connection.idle.max", poolMax))
        poolConfig.setMinIdle(Platform.getInteger("redis.connection.idle.min", math.min(4, poolMax)))
        poolConfig.setTestWhileIdle(true)
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(Platform.getLong("redis.connection.minEvictableIdleTimeSeconds", 120)).toMillis)
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(Platform.getLong("redis.connection.timeBetweenEvictionRunsSeconds", 300)).toMillis)
        poolConfig.setBlockWhenExhausted(true)
        poolConfig.setMaxWaitMillis(redis_timeout)
        poolConfig
    }

    @volatile private var sharedPool: JedisPool = _

    private def newPool(): JedisPool = {
        logger.info(null, s"RedisCacheUtil: creating shared JedisPool for $redis_host:$redis_port db=$index maxTotal=$poolMax timeout=$redis_timeout")
        new JedisPool(buildPoolConfig, redis_host, redis_port, redis_timeout, redis_password, index)
    }

    private[util] def pool: JedisPool = {
        if (sharedPool == null) synchronized { if (sharedPool == null) sharedPool = newPool() }
        sharedPool
    }

    /** Replace the shared pool (used by resetConnection). */
    private[util] def reset(): Unit = synchronized {
        val old = sharedPool
        sharedPool = newPool()
        if (old != null) old.close()
    }

    /** Close the shared pool for real — JVM shutdown only. */
    def shutdown(): Unit = synchronized {
        if (sharedPool != null) { sharedPool.close(); sharedPool = null }
    }
}

class RedisCacheUtil {

    private val logger: LoggerUtil = new LoggerUtil(classOf[RedisCacheUtil])

    implicit val className = "org.sunbird.cache.connector.RedisConnector"

    private val index: Int = Platform.getInteger("redis.dbIndex", 0)

    private def jedisPool: JedisPool = RedisCacheUtil.pool

    def getConnection(database: Int): Jedis = {
        val conn = jedisPool.getResource
        conn.select(database)
        conn
    }

    def getConnection: Jedis = try {
        val jedis = jedisPool.getResource
        if (index > 0) jedis.select(index)
        jedis
    } catch {
        case e: Exception => throw e
    }

    /**
      * This Method takes a connection object and put it back to pool.
      *
      * @param jedis
      */
    protected def returnConnection(jedis: Jedis): Unit = {
        try if (null != jedis) jedisPool.returnResource(jedis)
        catch {
            case e: Exception => throw e
        }
    }


    def resetConnection(): Unit = RedisCacheUtil.reset()

    /**
      * Intentionally a no-op: the pool is shared across the JVM and this is called from actor
      * postStop, which also runs on every actor restart. Use RedisCacheUtil.shutdown() at JVM exit.
      */
    def closePool() = {}

    def checkConnection = {
        try {
            val conn = getConnection(2)
            conn.close()
            true;
        } catch {
            case ex: Exception => false
        }
    }

    /**
      * This method store string data into cache for given Key
      *
      * @param key
      * @param data
      * @param ttl
      */
    def set(key: String, data: String, ttl: Int = 0): Unit = {
        val jedis = getConnection
        try {
            jedis.del(key)
            jedis.set(key, data)
            if (ttl > 0) jedis.expire(key, Int.box(ttl))
        } catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Saving String Data to Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * This method read string data from cache for a given key
      *
      * @param key
      * @param ttl
      * @param handler
      * @return
      */
    def get(key: String, handler: (String) => String = defaultStringHandler, ttl: Int = 0): String = {
        val jedis = getConnection
        try {
            var data = jedis.get(key)
            if (null != handler && (null == data || data.isEmpty)) {
                data = handler(key)
                if (null != data && !data.isEmpty)
                    set(key, data, ttl)
            }
            data
        }
        catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Fetching String Data from Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * This Method Returns Future[String] for given key
      *
      * @param key
      * @param asyncHandler
      * @param ttl
      * @param ec
      * @return Future[String]
      */
    def getAsync(key: String, asyncHandler: (String) => Future[String], ttl: Int = 0)(implicit ec: ExecutionContext): Future[String] = {
        val jedis = getConnection
        try {
            val data = jedis.get(key)
            if (null != asyncHandler && (null == data || data.isEmpty)) {
                val dataFuture: Future[String] = asyncHandler(key)
                dataFuture.map(value => {
                    if (null != value && !value.isEmpty)
                        set(key, value, ttl)
                    value
                })
            } else Future {
                data
            }
        }
        catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Fetching String Data from Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * This method increment the value by 1 into cache for given key and returns the new value
      *
      * @param key
      * @return Double
      */
    def incrementAndGet(key: String): Double = {
        val jedis = getConnection
        val inc = 1.0
        try jedis.incrByFloat(key, inc)
        catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Incrementing Value for Key : " + key + " | Exception is : ", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * This method store/save list data into cache for given Key
      *
      * @param key
      * @param data
      * @param isPartialUpdate
      * @param ttl
      */
    def saveList(key: String, data: List[String], ttl: Int = 0, isPartialUpdate: Boolean = false): Unit = {
        val jedis = getConnection
        try {
            if (!isPartialUpdate)
                jedis.del(key)
            data.foreach(entry => jedis.sadd(key, entry))
            if (ttl > 0 && !isPartialUpdate) jedis.expire(key, Int.box(ttl))
        } catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Saving List Data to Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * This method store/save list data into cache for given Key
      *
      * @param key
      * @param data
      */
    def addToList(key: String, data: List[String]): Unit = {
        saveList(key, data, 0, true)
    }

    /**
      * This method returns list data from cache for a given key
      *
      * @param key
      * @param handler
      * @param ttl
      * @return
      */
    def getList(key: String, handler: (String) => List[String] = defaultListHandler, ttl: Int = 0, index: Int = 0): List[String] = {
        val jedis = getConnection(index)
        try {
            var data = jedis.smembers(key).asScala.toList
            if (null != handler && (null == data || data.isEmpty)) {
                data = handler(key)
                if (null != data && !data.isEmpty)
                    saveList(key, data, ttl, false)
            }
            data
        } catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Fetching List Data from Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * This method returns list data from cache for a given key
      *
      * @param key
      * @param asyncHandler
      * @param ttl
      * @param ec
      * @return Future[List[String]]
      **/
    def getListAsync(key: String, asyncHandler: (String) => Future[List[String]], ttl: Int = 0)(implicit ec: ExecutionContext): Future[List[String]] = {
        val jedis = getConnection
        try {
            val data = jedis.smembers(key).asScala.toList
            if (null != asyncHandler && (null == data || data.isEmpty)) {
                val dataFuture = asyncHandler(key)
                dataFuture.map(value => {
                    if (null != value && !value.isEmpty)
                        saveList(key, value, ttl, false)
                    value
                })
            } else Future {
                data
            }
        } catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Fetching List Data from Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * This Method Remove Given Data From Existing List For Given Key
      *
      * @param key
      * @param data
      */
    def removeFromList(key: String, data: List[String]): Unit = {
        val jedis = getConnection
        try data.foreach(entry => jedis.srem(key, entry))
        catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Deleting Partial Data From Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * This method delete data from cache for given key/keys
      *
      * @param keys
      */
    def delete(keys: String*): Unit = {
        val jedis = getConnection
        try jedis.del(keys.map(_.asInstanceOf[String]): _*)
        catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Deleting Records From Redis Cache for Identifiers : " + keys.toArray + " | Exception is : ", e)
                throw e
        } finally returnConnection(jedis)
    }

    def deleteKey(key: String): Unit = {
        if (StringUtils.isNotBlank(key)) {
            val jedis = getConnection
            try jedis.del(key)
            catch {
                case e: Exception =>
                    logger.error(null, "Exception Occurred While Deleting Record From Redis Cache for Identifier : " + key + " | Exception is : ", e)
                    throw e
            } finally returnConnection(jedis)
        }
    }

    /**
      * This method delete data from cache for all key/keys matched with given pattern
      *
      * @param pattern
      */
    def deleteByPattern(pattern: String): Unit = {
        if (StringUtils.isNotBlank(pattern) && !StringUtils.equalsIgnoreCase(pattern, "*")) {
            val jedis = getConnection
            try {
                val keys = jedis.keys(pattern)
                if (keys != null && keys.size > 0)
                  keys.asScala.foreach(k => jedis.del(k))
            } catch {
                case e: Exception =>
                    logger.error(null, "Exception Occurred While Deleting Records From Redis Cache for Pattern : " + pattern + " | Exception is : ", e)
                    throw e
            } finally returnConnection(jedis)
        }
    }

    private def defaultStringHandler(objKey: String): String = {
        //Default Implementation Can Be Provided Here
        ""
    }

    private def defaultListHandler(objKey: String): List[String] = {
        //Default Implementation Can Be Provided Here
        List()
    }

    def getList(key: String, index: Int): List[String] = getList(key, defaultListHandler, 0, index)

    /**
      * This method read string data from cache for a given key
      *
      * @param key
      * @param ttl
      * @param handler
      * @return
      */
    /**
      * MGET for many keys on one DB index in a single round-trip. Returns key -> value for keys that
      * exist; missing keys are absent from the result. Throws on connection failure (caller decides).
      */
    def mgetUsingIndex(keys: java.util.List[String], index: Int): java.util.Map[String, String] = {
        val result = new java.util.HashMap[String, String]()
        if (keys == null || keys.isEmpty) return result
        val jedis = getConnection(index)
        try {
            val values = jedis.mget(keys.asScala: _*)
            var i = 0
            while (i < keys.size()) {
                val v = values.get(i)
                if (v != null) result.put(keys.get(i), v)
                i += 1
            }
            result
        } catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While MGET of " + keys.size() + " keys from Redis Cache | Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    /**
      * Members of a SET key, or field names of a HASH key, on the given DB index. Empty list when the
      * key is absent or of another type. Throws on connection failure (caller decides).
      */
    def getIdsUsingIndex(key: String, index: Int): java.util.List[String] = {
        val jedis = getConnection(index)
        try {
            jedis.`type`(key) match {
                case "set" => new java.util.ArrayList[String](jedis.smembers(key))
                case "hash" => new java.util.ArrayList[String](jedis.hkeys(key))
                case other =>
                    if (other != "none") logger.info(null, s"RedisCacheUtil: key $key has type $other, expected set or hash")
                    new java.util.ArrayList[String]()
            }
        } catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While reading ids from Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }

    def getUsingIndex(key: String, handler: (String) => String = defaultStringHandler, ttl: Int = 0, index: Int): String = {
        val jedis = getConnection(index)
        try {
            var data = jedis.get(key)
            if (null != handler && (null == data || data.isEmpty)) {
                data = handler(key)
                if (null != data && !data.isEmpty)
                    set(key, data, ttl)
            }
            data
        }
        catch {
            case e: Exception =>
                logger.error(null, "Exception Occurred While Fetching String Data from Redis Cache for Key : " + key + "| Exception is:", e)
                throw e
        } finally returnConnection(jedis)
    }
}
