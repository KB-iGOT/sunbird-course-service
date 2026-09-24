package org.sunbird.learner.actors.accesssettings;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.sunbird.learner.actors.accesssettings.model.UserGroup;
import org.sunbird.learner.actors.accesssettings.model.UserGroupCriteria;

public class RuleEngineValidatorTest {

  private UserGroupCriteria criteria(String key, String... values) {
    UserGroupCriteria c = new UserGroupCriteria();
    c.setCriteriaKey(key);
    c.setCriteriaValue(java.util.Arrays.asList(values));
    return c;
  }

  private UserGroup group(UserGroupCriteria... criteriaList) {
    UserGroup g = new UserGroup();
    g.setUserGroupId("group-" + System.identityHashCode(criteriaList));
    g.setUserGroupCriteriaList(new ArrayList<>(java.util.Arrays.asList(criteriaList)));
    return g;
  }

  @Test
  public void emptyRules_returnsFalse() {
    Map<String, String> userAttrs = new HashMap<>();
    userAttrs.put("rootorgid", "ORG_001");

    boolean result = RuleEngineValidator.getInstance().evaluateRules(userAttrs, Collections.emptyList());

    assertFalse(result);
  }

  @Test
  public void nullRules_returnsFalseNotException() {
    Map<String, String> userAttrs = new HashMap<>();
    userAttrs.put("rootorgid", "ORG_001");

    boolean result = RuleEngineValidator.getInstance().evaluateRules(userAttrs, null);

    assertFalse(result);
  }

  @Test
  public void nullCriteriaListOnRule_treatedAsNoCriteria_returnsTrue() {
    Map<String, String> userAttrs = new HashMap<>();
    userAttrs.put("rootorgid", "ORG_001");

    UserGroup groupWithNullCriteria = new UserGroup();
    groupWithNullCriteria.setUserGroupId("group-null-criteria");
    groupWithNullCriteria.setUserGroupCriteriaList(null);

    boolean result = RuleEngineValidator.getInstance()
        .evaluateRules(userAttrs, Collections.singletonList(groupWithNullCriteria));

    assertTrue(result);
  }

  @Test
  public void singleGroup_allCriteriaMatch_returnsTrue() {
    Map<String, String> userAttrs = new HashMap<>();
    userAttrs.put("rootorgid", "ORG_001");
    userAttrs.put("designation", "Officer");

    List<UserGroup> groups = Collections.singletonList(
        group(criteria("rootorgid", "org_001"), criteria("designation", "officer")));

    boolean result = RuleEngineValidator.getInstance().evaluateRules(userAttrs, groups);

    assertTrue(result);
  }

  @Test
  public void singleGroup_oneCriterionFails_wholeGroupFails() {
    Map<String, String> userAttrs = new HashMap<>();
    userAttrs.put("rootorgid", "ORG_001");
    userAttrs.put("designation", "Manager");

    List<UserGroup> groups = Collections.singletonList(
        group(criteria("rootorgid", "org_001"), criteria("designation", "officer")));

    boolean result = RuleEngineValidator.getInstance().evaluateRules(userAttrs, groups);

    assertFalse(result);
  }

  @Test
  public void secondGroupMatches_whenFirstGroupFails_returnsTrue() {
    Map<String, String> userAttrs = new HashMap<>();
    userAttrs.put("rootorgid", "ORG_002");

    List<UserGroup> groups = java.util.Arrays.asList(
        group(criteria("rootorgid", "org_001")),
        group(criteria("rootorgid", "org_002")));

    boolean result = RuleEngineValidator.getInstance().evaluateRules(userAttrs, groups);

    assertTrue(result);
  }

  @Test
  public void noGroupMatches_returnsFalse() {
    Map<String, String> userAttrs = new HashMap<>();
    userAttrs.put("rootorgid", "ORG_003");

    List<UserGroup> groups = java.util.Arrays.asList(
        group(criteria("rootorgid", "org_001")),
        group(criteria("rootorgid", "org_002")));

    boolean result = RuleEngineValidator.getInstance().evaluateRules(userAttrs, groups);

    assertFalse(result);
  }

  @Test
  public void userMissingAttribute_criterionFails() {
    Map<String, String> userAttrs = new HashMap<>();
    // "rootorgid" intentionally absent

    List<UserGroup> groups = Collections.singletonList(group(criteria("rootorgid", "org_001")));

    boolean result = RuleEngineValidator.getInstance().evaluateRules(userAttrs, groups);

    assertFalse(result);
  }
}
