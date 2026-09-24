package org.sunbird.learner.util;

/**
 * Signals that the CbPlan dictionary could not be fetched or parsed, as distinct from a
 * successfully-fetched dictionary that simply has no plan for the given do_id. Callers must not
 * treat this the same as "not eligible" - the former is a system/lookup failure, the latter is a
 * legitimate outcome.
 */
public class CbPlanLookupException extends RuntimeException {

  public CbPlanLookupException(String message, Throwable cause) {
    super(message, cause);
  }
}
