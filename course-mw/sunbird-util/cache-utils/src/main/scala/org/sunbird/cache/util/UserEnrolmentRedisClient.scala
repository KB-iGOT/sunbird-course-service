package org.sunbird.cache.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.common.models.util.{LoggerUtil, PropertiesCache}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.time.Duration
import scala.collection.JavaConverters._

/**
  * Client for the dedicated user-enrolment Redis (keys course-enrolments:v1:{userId}).
  * One pool per JVM, bound to a fixed DB index (no SELECT per checkout).
  * Every operation is best-effort: a Redis failure is logged and reported as a miss/false,
  * never thrown, so callers fall back to Cassandra.
  *
  * Configuration: externalresource.properties, each key overridable by an environment variable of
  * the same name (PropertiesCache semantics):
  *   user_enrolment_cache_enabled, user_enrolment_cache_ttl_seconds,
  *   user_enrolment_redis_host/port/index/pool_max/timeout_ms,
  *   user_enrolment_redis_password_enabled (true: authenticate with user_enrolment_redis_username/password)
  */
object UserEnrolmentRedisClient {

  private val logger: LoggerUtil = new LoggerUtil(UserEnrolmentRedisClient.getClass)

  private val props = PropertiesCache.getInstance

  /** PropertiesCache.getProperty returns the key itself when unset; treat that as "not configured". */
  private def prop(key: String, default: String): String = {
    val v = props.getProperty(key)
    if (StringUtils.isBlank(v) || v == key) default else v.trim
  }
  private def intProp(key: String, default: Int): Int =
    try prop(key, default.toString).toInt catch { case _: NumberFormatException => default }

  val enabled: Boolean = "true".equalsIgnoreCase(prop("user_enrolment_cache_enabled", "false"))
  val ttlSeconds: Int = intProp("user_enrolment_cache_ttl_seconds", 86400)
  private val ttl: Long = ttlSeconds.toLong // Jedis overloads expire(String,int)/(String,long); a scala.Long picks the long one

  private val host: String = prop("user_enrolment_redis_host", "127.0.0.1")
  private val port: Int = intProp("user_enrolment_redis_port", 6379)
  private val index: Int = intProp("user_enrolment_redis_index", 0)
  private val passwordEnabled: Boolean = "true".equalsIgnoreCase(prop("user_enrolment_redis_password_enabled", "false"))
  private val username: String = if (passwordEnabled) prop("user_enrolment_redis_username", null) else null
  private val password: String = if (passwordEnabled) prop("user_enrolment_redis_password", null) else null
  private val poolMax: Int = intProp("user_enrolment_redis_pool_max", 64)
  private val timeoutMs: Int = intProp("user_enrolment_redis_timeout_ms", 2000)

  private lazy val pool: JedisPool = {
    val cfg = new JedisPoolConfig
    cfg.setMaxTotal(poolMax)
    cfg.setMaxIdle(poolMax)
    cfg.setMinIdle(math.min(4, poolMax))
    cfg.setTestWhileIdle(true)
    cfg.setMinEvictableIdleTimeMillis(Duration.ofSeconds(120).toMillis)
    cfg.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(300).toMillis)
    cfg.setBlockWhenExhausted(true)
    cfg.setMaxWaitMillis(timeoutMs)
    logger.info(null, s"UserEnrolmentRedisClient: connecting to $host:$port db=$index poolMax=$poolMax auth=$passwordEnabled")
    if (!passwordEnabled) new JedisPool(cfg, host, port, timeoutMs, null, index)
    else if (StringUtils.isBlank(username)) new JedisPool(cfg, host, port, timeoutMs, password, index)
    else new JedisPool(cfg, host, port, timeoutMs, username, password, index)
  }

  private def withJedis[T](op: String, key: String)(f: Jedis => T): Option[T] = {
    if (!enabled) return None
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      Some(f(jedis))
    } catch {
      case e: Exception =>
        logger.error(null, s"UserEnrolmentRedisClient: $op failed for key $key: ${e.getMessage}", e)
        None
    } finally {
      if (jedis != null) jedis.close()
    }
  }

  /** HGETALL + sliding EXPIRE. None = disabled, error, or key absent. */
  def hgetAll(key: String): Option[java.util.Map[String, String]] =
    withJedis("hgetAll", key) { j =>
      val m = j.hgetAll(key)
      if (m == null || m.isEmpty) null
      else { j.expire(key, ttl); m }
    }.flatMap(Option(_))

  /** Replace the whole hash in one pipeline: DEL, HSET fields, EXPIRE. */
  def hsetAll(key: String, fields: java.util.Map[String, String]): Boolean =
    withJedis("hsetAll", key) { j =>
      val p = j.pipelined()
      p.del(Array(key): _*)
      if (!fields.isEmpty) p.hset(key, fields)
      p.expire(key, ttl)
      p.sync()
      true
    }.getOrElse(false)

  /** HSET one field only if the hash already exists (never materialises a partial user). */
  def hsetIfExists(key: String, field: String, value: String): Boolean =
    withJedis("hsetIfExists", key) { j =>
      if (j.exists(key)) { j.hset(key, field, value); j.expire(key, ttl); true } else false
    }.getOrElse(false)

  def hdel(key: String, field: String): Boolean =
    withJedis("hdel", key) { j => j.hdel(key, field); true }.getOrElse(false)

  def del(key: String): Boolean =
    withJedis("del", key) { j => j.del(key); true }.getOrElse(false)

  def closePool(): Unit = if (enabled) pool.close()
}
