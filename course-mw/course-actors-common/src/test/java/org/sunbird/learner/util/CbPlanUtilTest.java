package org.sunbird.learner.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.models.util.HttpUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HttpUtil.class})
@PowerMockIgnore({"javax.management.*"})
public class CbPlanUtilTest {

  @Before
  public void setUp() {
    PowerMockito.mockStatic(HttpUtil.class);
  }

  // ---------- getCbPlanDictionary ----------

  @Test
  public void getCbPlanDictionary_success_parsesResponse() throws Exception {
    when(HttpUtil.sendPostRequest(anyString(), anyString(), any()))
        .thenReturn("{\"result\":{\"2026-27\":{\"aparPlanCount\":1}}}");

    Map<String, Object> result = CbPlanUtil.getCbPlanDictionary(new HashMap<>(), null);

    assertTrue(result.containsKey("result"));
  }

  @Test
  public void getCbPlanDictionary_emptyResponse_returnsEmptyMap() throws Exception {
    when(HttpUtil.sendPostRequest(anyString(), anyString(), any())).thenReturn("");

    Map<String, Object> result = CbPlanUtil.getCbPlanDictionary(new HashMap<>(), null);

    assertTrue(result.isEmpty());
  }

  @Test
  public void getCbPlanDictionary_httpUtilThrows_failsClosedToEmptyMap() throws Exception {
    when(HttpUtil.sendPostRequest(anyString(), anyString(), any())).thenThrow(new RuntimeException("timeout"));

    Map<String, Object> result = CbPlanUtil.getCbPlanDictionary(new HashMap<>(), null);

    assertTrue(result.isEmpty());
  }

  // ---------- findPlanForComprehensiveAssessment ----------

  @Test
  public void findPlanForComprehensiveAssessment_matchInAparPlanList() {
    Map<String, Object> plan = planWithCA("do_ca_1");
    Map<String, Object> dictionary = dictionaryWith("aparPlanList", plan);

    Map<String, Object> found = CbPlanUtil.findPlanForComprehensiveAssessment(dictionary, "do_ca_1");

    assertEquals(plan, found);
  }

  @Test
  public void findPlanForComprehensiveAssessment_matchInNonAparPlanList() {
    Map<String, Object> plan = planWithCA("do_ca_2");
    Map<String, Object> dictionary = dictionaryWith("nonAparPlanList", plan);

    Map<String, Object> found = CbPlanUtil.findPlanForComprehensiveAssessment(dictionary, "do_ca_2");

    assertEquals(plan, found);
  }

  @Test
  public void findPlanForComprehensiveAssessment_noMatch_returnsNull() {
    Map<String, Object> plan = planWithCA("do_ca_other");
    Map<String, Object> dictionary = dictionaryWith("aparPlanList", plan);

    Map<String, Object> found = CbPlanUtil.findPlanForComprehensiveAssessment(dictionary, "do_ca_not_present");

    assertNull(found);
  }

  @Test
  public void findPlanForComprehensiveAssessment_missingResultKey_returnsNullNotException() {
    Map<String, Object> dictionary = new HashMap<>();

    Map<String, Object> found = CbPlanUtil.findPlanForComprehensiveAssessment(dictionary, "do_ca_1");

    assertNull(found);
  }

  // ---------- getMandatoryCourseIds ----------

  @Test
  public void getMandatoryCourseIds_extractsOnlyMandatoryEntries() {
    Map<String, Object> plan = new HashMap<>();
    List<Object> contentList = new ArrayList<>();
    contentList.add(contentEntry("do_mandatory_1", true));
    contentList.add(contentEntry("do_optional_1", false));
    contentList.add(contentEntry("do_mandatory_2", true));
    plan.put("contentList", contentList);

    List<String> mandatoryIds = CbPlanUtil.getMandatoryCourseIds(plan);

    assertEquals(2, mandatoryIds.size());
    assertTrue(mandatoryIds.contains("do_mandatory_1"));
    assertTrue(mandatoryIds.contains("do_mandatory_2"));
  }

  @Test
  public void getMandatoryCourseIds_noContentList_returnsEmptyList() {
    Map<String, Object> plan = new HashMap<>();

    List<String> mandatoryIds = CbPlanUtil.getMandatoryCourseIds(plan);

    assertTrue(mandatoryIds.isEmpty());
  }

  // ---------- fixtures ----------

  private Map<String, Object> planWithCA(String caId) {
    Map<String, Object> plan = new HashMap<>();
    plan.put("comprehensiveAssessment", caId);
    return plan;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> dictionaryWith(String listKey, Map<String, Object> plan) {
    Map<String, Object> planMap = new HashMap<>();
    planMap.put("plan1", plan);
    Map<String, Object> yearEntry = new HashMap<>();
    yearEntry.put(listKey, planMap);
    Map<String, Object> result = new HashMap<>();
    result.put("2026-27", yearEntry);
    Map<String, Object> dictionary = new HashMap<>();
    dictionary.put("result", result);
    return dictionary;
  }

  private Map<String, Object> contentEntry(String identifier, boolean mandatory) {
    Map<String, Object> entry = new HashMap<>();
    entry.put("identifier", identifier);
    entry.put("mandatory", mandatory);
    return entry;
  }
}
