package org.sunbird.learner.actors.accesssettings.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true) 
public class AccessControl {
     @JsonProperty("version")  // Map JSON version to this field
    private int version;

    @JsonProperty("userGroups")  // Map JSON userGroups to this field
    private List<UserGroup> userGroups;

    // Getters and setters
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public List<UserGroup> getUserGroups() {
        return userGroups;
    }

    public void setUserGroups(List<UserGroup> userGroups) {
        this.userGroups = userGroups;
    }
}

