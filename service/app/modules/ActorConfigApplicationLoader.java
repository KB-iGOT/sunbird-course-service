package modules;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.PropertiesCache;
import play.ApplicationLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import play.inject.guice.GuiceApplicationBuilder;
import play.inject.guice.GuiceApplicationLoader;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Overlays actor pool sizes and dispatcher thread caps from externalresource.properties (or the
 * environment variable of the same name, per PropertiesCache) onto application.conf, so these
 * values can be tuned per deployment without a code change. A key that is unset, blank or not a
 * positive integer leaves the application.conf value untouched.
 */
public class ActorConfigApplicationLoader extends GuiceApplicationLoader {

  /** property key -> akka config path */
  private static final Map<String, String> SETTINGS = new LinkedHashMap<>();
  static {
    actor("extended_course_enrolment_actor_instances", "extended-course-enrolment-actor");
    actor("extended_badge_enrolment_actor_instances", "extended-badge-enrolment-actor");
    actor("event_management_actor_instances", "event-management-actor");
    actor("event_consumption_actor_instances", "event-consumption-actor");
    actor("content_consumption_actor_instances", "content-consumption-actor");
    actor("extended_content_consumption_actor_instances", "extended-content-consumption-actor");
    actor("course_enrolment_actor_instances", "course-enrolment-actor");
    actor("course_enrolment_actor_v3_instances", "course-enrolment-actor-v3");
    actor("course_batch_management_actor_instances", "course-batch-management-actor");
    actor("eventset_enrolment_actor_instances", "eventset-enrolment-actor");
    actor("event_batch_management_actor_instances", "event-batch-management-actor");
    dispatcher("extended_dispatcher_max_threads", "extended-dispatcher");
    dispatcher("rr_dispatcher_max_threads", "rr-dispatcher");
    dispatcher("tracking_dispatcher_max_threads", "tracking-dispatcher");
    dispatcher("default_dispatcher_max_threads", "default-dispatcher");
  }

  private static void actor(String prop, String actorName) {
    SETTINGS.put(prop, "akka.actor.deployment.\"/" + actorName + "\".nr-of-instances");
  }

  private static void dispatcher(String prop, String dispatcherName) {
    SETTINGS.put(prop, "akka.actor." + dispatcherName + ".fork-join-executor.parallelism-max");
  }

  @Override
  public GuiceApplicationBuilder builder(ApplicationLoader.Context context) {
    Map<String, Object> overrides = new HashMap<>();
    PropertiesCache props = PropertiesCache.getInstance();
    for (Map.Entry<String, String> e : SETTINGS.entrySet()) {
      Integer v = positiveInt(props.getProperty(e.getKey()), e.getKey());
      if (v != null) {
        overrides.put(e.getValue(), v);
        System.out.println("ActorConfigApplicationLoader: " + e.getValue() + " = " + v + " (from " + e.getKey() + ")");
      }
    }
    Config merged = overrides.isEmpty()
        ? context.initialConfig()
        : ConfigFactory.parseMap(overrides).withFallback(context.initialConfig());
    return initialBuilder.in(context.environment()).loadConfig(merged).overrides(overrides(context));
  }

  /** null when unset (PropertiesCache returns the key itself), blank, non-numeric or <= 0. */
  private static Integer positiveInt(String value, String key) {
    if (StringUtils.isBlank(value) || key.equals(value)) return null;
    try {
      int n = Integer.parseInt(value.trim());
      return n > 0 ? n : null;
    } catch (NumberFormatException e) {
      System.err.println("ActorConfigApplicationLoader: ignoring non-numeric " + key + "=" + value);
      return null;
    }
  }
}
