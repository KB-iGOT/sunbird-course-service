package org.sunbird.learner.actors.accesssettings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.RequestContext;
import org.sunbird.learner.actors.accesssettings.dao.impl.AccessSettingsDaoImpl;
import org.sunbird.learner.actors.accesssettings.model.AccessControl;
import org.sunbird.learner.actors.accesssettings.model.UserGroup;
import org.sunbird.learner.actors.accesssettings.model.UserGroupCriteria;
import org.sunbird.userorg.UserOrgServiceImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AccessSettingsDaoImpl.class, UserOrgServiceImpl.class})
@PowerMockIgnore({"javax.management.*"})
public class AccessSettingsUtilTest {

  private static final String COURSE_ID = "do_mandatory_1";
  private static final String USER_ID = "user1";

  private AccessSettingsDaoImpl accessSettingsDao;
  private UserOrgServiceImpl userOrgService;

  @Before
  public void setUp() {
    PowerMockito.mockStatic(AccessSettingsDaoImpl.class);
    accessSettingsDao = mock(AccessSettingsDaoImpl.class);
    when(AccessSettingsDaoImpl.getInstance()).thenReturn(accessSettingsDao);

    PowerMockito.mockStatic(UserOrgServiceImpl.class);
    userOrgService = mock(UserOrgServiceImpl.class);
    when(UserOrgServiceImpl.getInstance()).thenReturn(userOrgService);
  }

  // ---------- isUserEligibleForAccessSettings ----------

  @Test
  public void whenAccessSettingsNotEnabled_thenEligibleWithoutAnyLookup() throws Exception {
    Map<String, Object> courseDetails = new HashMap<>();
    courseDetails.put(JsonKey.ACCESS_SETTINGS_ENABLED, false);

    boolean result = AccessSettingsUtil.isUserEligibleForAccessSettings(
        mockContext(), courseDetails, COURSE_ID, USER_ID);

    assertTrue(result);
    Mockito.verifyNoMoreInteractions(accessSettingsDao);
  }

  @Test
  public void whenAccessSettingsFlagAbsent_thenEligibleWithoutAnyLookup() throws Exception {
    Map<String, Object> courseDetails = new HashMap<>();

    boolean result = AccessSettingsUtil.isUserEligibleForAccessSettings(
        mockContext(), courseDetails, COURSE_ID, USER_ID);

    assertTrue(result);
    Mockito.verifyNoMoreInteractions(accessSettingsDao);
  }

  @Test
  public void whenEnabledButNoAccessControlRowFound_thenFailsClosed() {
    Map<String, Object> courseDetails = enabledCourseDetails();
    when(accessSettingsDao.readAccessSettings(any(), anyString())).thenReturn(null);

    boolean result = AccessSettingsUtil.isUserEligibleForAccessSettings(
        mockContext(), courseDetails, COURSE_ID, USER_ID);

    assertFalse(result);
  }

  @Test
  public void whenUserProfileCannotBeFetched_thenFailsClosed() throws Exception {
    Map<String, Object> courseDetails = enabledCourseDetails();
    when(accessSettingsDao.readAccessSettings(any(), anyString()))
        .thenReturn(accessControlWithRootOrgGroup("ORG_001"));
    when(userOrgService.getUserDetailsById(anyString(), any())).thenThrow(new RuntimeException("boom"));

    boolean result = AccessSettingsUtil.isUserEligibleForAccessSettings(
        mockContext(), courseDetails, COURSE_ID, USER_ID);

    assertFalse(result);
  }

  @Test
  public void whenUserProfileEmpty_thenFailsClosed() throws Exception {
    Map<String, Object> courseDetails = enabledCourseDetails();
    when(accessSettingsDao.readAccessSettings(any(), anyString()))
        .thenReturn(accessControlWithRootOrgGroup("ORG_001"));
    when(userOrgService.getUserDetailsById(anyString(), any())).thenReturn(new HashMap<>());

    boolean result = AccessSettingsUtil.isUserEligibleForAccessSettings(
        mockContext(), courseDetails, COURSE_ID, USER_ID);

    assertFalse(result);
  }

  @Test
  public void whenUserMatchesConfiguredOrg_thenEligible() throws Exception {
    Map<String, Object> courseDetails = enabledCourseDetails();
    when(accessSettingsDao.readAccessSettings(any(), anyString()))
        .thenReturn(accessControlWithRootOrgGroup("ORG_001"));
    Map<String, Object> profile = new HashMap<>();
    profile.put(JsonKey.ID, USER_ID);
    profile.put(JsonKey.ROOT_ORG_ID, "ORG_001");
    when(userOrgService.getUserDetailsById(anyString(), any())).thenReturn(profile);

    boolean result = AccessSettingsUtil.isUserEligibleForAccessSettings(
        mockContext(), courseDetails, COURSE_ID, USER_ID);

    assertTrue(result);
  }

  @Test
  public void whenUserDoesNotMatchAnyGroup_thenIneligible() throws Exception {
    Map<String, Object> courseDetails = enabledCourseDetails();
    when(accessSettingsDao.readAccessSettings(any(), anyString()))
        .thenReturn(accessControlWithRootOrgGroup("ORG_001"));
    Map<String, Object> profile = new HashMap<>();
    profile.put(JsonKey.ID, USER_ID);
    profile.put(JsonKey.ROOT_ORG_ID, "ORG_002");
    when(userOrgService.getUserDetailsById(anyString(), any())).thenReturn(profile);

    boolean result = AccessSettingsUtil.isUserEligibleForAccessSettings(
        mockContext(), courseDetails, COURSE_ID, USER_ID);

    assertFalse(result);
  }

  // ---------- getUserAttributes ----------

  @Test
  public void getUserAttributes_fullProfile_mapsAllFields() {
    Map<String, Object> profile = new HashMap<>();
    profile.put(JsonKey.ID, USER_ID);
    profile.put(JsonKey.ROOT_ORG_ID, "ORG_001");
    profile.put(JsonKey.PROFILE_DETAILS,
        "{\"profileStatus\":\"VERIFIED\","
            + "\"professionalDetails\":[{\"designation\":\"Officer\",\"group\":\"Group A\"}],"
            + "\"cadreDetails\":{\"cadreName\":\"IAS\",\"civilServiceName\":\"CSS\",\"cadreBatch\":2020,\"isOnCentralDeputation\":true}}");

    Map<String, String> attrs = AccessSettingsUtil.getUserAttributes(profile);

    assertEquals(USER_ID, attrs.get(JsonKey.USER));
    assertEquals("ORG_001", attrs.get(JsonKey.ROOT_ORG_ID.toLowerCase()));
    assertEquals("VERIFIED", attrs.get(JsonKey.PROFILE_STATUS.toLowerCase()));
    assertEquals("Officer", attrs.get(JsonKey.DESIGNATION));
    assertEquals("Group A", attrs.get(JsonKey.GROUP));
    assertEquals("IAS", attrs.get(JsonKey.CADRE));
    assertEquals("CSS", attrs.get(JsonKey.SERVICE));
    assertEquals("2020", attrs.get(JsonKey.BATCH));
    assertEquals("true", attrs.get(JsonKey.IS_ON_CENTRAL_DEPUTATION.toLowerCase()));
  }

  @Test
  public void getUserAttributes_blankProfileDetails_onlyBasicFieldsPopulated() {
    Map<String, Object> profile = new HashMap<>();
    profile.put(JsonKey.ID, USER_ID);
    profile.put(JsonKey.ROOT_ORG_ID, "ORG_001");

    Map<String, String> attrs = AccessSettingsUtil.getUserAttributes(profile);

    assertEquals(USER_ID, attrs.get(JsonKey.USER));
    assertEquals("ORG_001", attrs.get(JsonKey.ROOT_ORG_ID.toLowerCase()));
    assertNull(attrs.get(JsonKey.DESIGNATION));
    assertNull(attrs.get(JsonKey.CADRE));
  }

  @Test(expected = ProjectCommonException.class)
  public void getUserAttributes_malformedProfileDetails_throws() {
    Map<String, Object> profile = new HashMap<>();
    profile.put(JsonKey.ID, USER_ID);
    profile.put(JsonKey.PROFILE_DETAILS, "{not-valid-json");

    AccessSettingsUtil.getUserAttributes(profile);
  }

  // ---------- fixtures ----------

  private RequestContext mockContext() {
    return mock(RequestContext.class);
  }

  private Map<String, Object> enabledCourseDetails() {
    Map<String, Object> courseDetails = new HashMap<>();
    courseDetails.put(JsonKey.ACCESS_SETTINGS_ENABLED, true);
    return courseDetails;
  }

  private AccessControl accessControlWithRootOrgGroup(String allowedOrgId) {
    UserGroupCriteria criteria = new UserGroupCriteria();
    criteria.setCriteriaKey(JsonKey.ROOT_ORG_ID.toLowerCase());
    criteria.setCriteriaValue(Collections.singletonList(allowedOrgId));

    UserGroup group = new UserGroup();
    group.setUserGroupId("group1");
    List<UserGroupCriteria> criteriaList = new ArrayList<>();
    criteriaList.add(criteria);
    group.setUserGroupCriteriaList(criteriaList);

    AccessControl accessControl = new AccessControl();
    accessControl.setVersion(1);
    List<UserGroup> groups = new ArrayList<>();
    groups.add(group);
    accessControl.setUserGroups(groups);
    return accessControl;
  }
}
