package org.sunbird.learner.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

  // ---------- fetchComprehensiveAssessmentEligibility ----------

  @Test
  public void fetchComprehensiveAssessmentEligibility_eligible_parsesResult() throws Exception {
    when(HttpUtil.sendGetRequest(anyString(), any())).thenReturn(
        "{\"result\":{\"eligible\":true,\"mandatoryCourses\":[\"do_mandatory_1\",\"do_mandatory_2\"]}}");

    Map<String, Object> result =
        CbPlanUtil.fetchComprehensiveAssessmentEligibility("do_ca_1", new HashMap<>(), null);

    assertEquals(Boolean.TRUE, result.get("eligible"));
    List<String> mandatoryCourses = (List<String>) result.get("mandatoryCourses");
    assertEquals(2, mandatoryCourses.size());
    assertTrue(mandatoryCourses.contains("do_mandatory_1"));
    assertTrue(mandatoryCourses.contains("do_mandatory_2"));
  }

  @Test
  public void fetchComprehensiveAssessmentEligibility_notEligible_parsesResult() throws Exception {
    when(HttpUtil.sendGetRequest(anyString(), any())).thenReturn(
        "{\"result\":{\"eligible\":false,\"mandatoryCourses\":[]}}");

    Map<String, Object> result =
        CbPlanUtil.fetchComprehensiveAssessmentEligibility("do_ca_1", new HashMap<>(), null);

    assertEquals(Boolean.FALSE, result.get("eligible"));
    assertTrue(((List<String>) result.get("mandatoryCourses")).isEmpty());
  }

  @Test(expected = CbPlanLookupException.class)
  public void fetchComprehensiveAssessmentEligibility_emptyResponse_throwsLookupException() throws Exception {
    when(HttpUtil.sendGetRequest(anyString(), any())).thenReturn("");

    CbPlanUtil.fetchComprehensiveAssessmentEligibility("do_ca_1", new HashMap<>(), null);
  }

  @Test(expected = CbPlanLookupException.class)
  public void fetchComprehensiveAssessmentEligibility_httpUtilThrows_throwsLookupException() throws Exception {
    when(HttpUtil.sendGetRequest(anyString(), any())).thenThrow(new RuntimeException("timeout"));

    CbPlanUtil.fetchComprehensiveAssessmentEligibility("do_ca_1", new HashMap<>(), null);
  }

  @Test(expected = CbPlanLookupException.class)
  public void fetchComprehensiveAssessmentEligibility_missingResultKey_throwsLookupException() throws Exception {
    when(HttpUtil.sendGetRequest(anyString(), any())).thenReturn("{\"id\":\"user.cbplan.eligibility\"}");

    CbPlanUtil.fetchComprehensiveAssessmentEligibility("do_ca_1", new HashMap<>(), null);
  }

  @Test(expected = CbPlanLookupException.class)
  public void fetchComprehensiveAssessmentEligibility_nonMapResult_throwsLookupException() throws Exception {
    when(HttpUtil.sendGetRequest(anyString(), any())).thenReturn("{\"result\":\"not-a-map\"}");

    CbPlanUtil.fetchComprehensiveAssessmentEligibility("do_ca_1", new HashMap<>(), null);
  }
}
