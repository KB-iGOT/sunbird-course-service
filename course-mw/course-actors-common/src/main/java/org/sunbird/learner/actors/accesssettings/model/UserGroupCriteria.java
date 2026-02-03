package org.sunbird.learner.actors.accesssettings.model;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserGroupCriteria {
    @JsonProperty("criteriaKey")
    private String criteriaKey;

    @JsonProperty("criteriaValue")
    private Set<String> criteriaValue;

    public UserGroupCriteria() {
    }

    public UserGroupCriteria(String criteriaKey, Set<String> criteriaValue) {
        this.criteriaKey = criteriaKey;
        this.criteriaValue = criteriaValue;
    }

    public boolean evaluate(Map<String, String> userAttributes) {
        String userValue = userAttributes.get(criteriaKey);
        if (StringUtils.isBlank(userValue)) {
            return false;
        }
        return criteriaValue.contains(userValue.toLowerCase());
    }

    public String getCriteriaKey() {
        return criteriaKey;
    }

    public void setCriteriaKey(String criteriaKey) {
        this.criteriaKey = criteriaKey.toLowerCase();
    }

    public Set<String> getCriteriaValue() {
        return criteriaValue;
    }

    public void setCriteriaValue(Object criteriaValue) {
        if (criteriaValue == null) {
            this.criteriaValue = new HashSet<>();
        } else if (criteriaValue instanceof Boolean) {
            this.criteriaValue = new HashSet<>();
            this.criteriaValue.add(String.valueOf(criteriaValue).toLowerCase());
        } else if (criteriaValue instanceof Collection) {
            this.criteriaValue = ((Collection<?>) criteriaValue).stream()
                .filter(Objects::nonNull)
                .map(obj -> obj.toString().toLowerCase())
                .collect(Collectors.toSet());
        } else {
            this.criteriaValue = new HashSet<>();
        }
    }
}
