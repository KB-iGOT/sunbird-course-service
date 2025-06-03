package org.sunbird.learner.actors.accesssettings.model;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserGroupCriteria {
    @JsonProperty("criteriaKey")
    private String criteriaKey;
    
    @JsonProperty("criteriaValue")
    private List<String> criteriaValue;

    public UserGroupCriteria() {
    }

    public UserGroupCriteria(String criteriaKey, List<String> criteriaValue) {
        this.criteriaKey = criteriaKey;
        this.criteriaValue = criteriaValue;
    }

    public boolean evaluate(Map<String, String> userAttributes) {
        String userValue = userAttributes.get(criteriaKey);
        if (userValue == null) {
            return false;
        }
        return criteriaValue.contains(userValue);
    }

    public String getCriteriaKey() {
        return criteriaKey;
    }

    public void setCriteriaKey(String criteriaKey) {
        this.criteriaKey = criteriaKey;
    }

    public List<String> getCriteriaValue() {
        return criteriaValue;
    }

    public void setCriteriaValue(List<String> criteriaValue) {
        this.criteriaValue = criteriaValue;
    }
}
