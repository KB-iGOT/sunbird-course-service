package org.sunbird.learner.actors.accesssettings.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserGroup {

    @JsonProperty("userGroupId")
    private String userGroupId;

    @JsonProperty("userGroupName")
    private String userGroupName;

    @JsonProperty("userGroupCriteriaList")
    private List<UserGroupCriteria> userGroupCriteriaList;

    // Getters and setters
    public String getUserGroupId() {
        return userGroupId;
    }

    public void setUserGroupId(String userGroupId) {
        this.userGroupId = userGroupId;
    }

    public String getUserGroupName() {
        return userGroupName;
    }

    public void setUserGroupName(String userGroupName) {
        this.userGroupName = userGroupName;
    }

    public List<UserGroupCriteria> getUserGroupCriteriaList() {
        return userGroupCriteriaList;
    }

    public void setUserGroupCriteriaList(List<UserGroupCriteria> userGroupCriteriaList) {
        this.userGroupCriteriaList = userGroupCriteriaList;
    }
}
