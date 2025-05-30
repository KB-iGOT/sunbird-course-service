package org.sunbird.learner.actors.accesssettings.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)  // Ignore any extra attributes in the JSON
public class UserGroup {

    @JsonProperty("ruleId")  // Map JSON ruleId to this field
    private String ruleId;

    @JsonProperty("ruleName")  // Map JSON ruleName to this field
    private String ruleName;

    @JsonProperty("userGroupCriteriaList")  // Map JSON userGroupCriteriaList to this field
    private List<UserGroupCriteria> userGroupCriteriaList;

    // Getters and setters
    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public List<UserGroupCriteria> getUserGroupCriteriaList() {
        return userGroupCriteriaList;
    }

    public void setUserGroupCriteriaList(List<UserGroupCriteria> userGroupCriteriaList) {
        this.userGroupCriteriaList = userGroupCriteriaList;
    }
}
