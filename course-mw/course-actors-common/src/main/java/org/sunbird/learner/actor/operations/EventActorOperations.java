package org.sunbird.learner.actor.operations;

public enum EventActorOperations {
    ISSUE_EVENT_CERTIFICATE("issueEventCertificate"),
    ADD_EVENT_BATCH_CERTIFICATE("addCertificateToEventBatch"),
    DELETE_EVENT_BATCH_CERTIFICATE("removeCertificateFromEventBatch");

    private String value;

  private EventActorOperations(String value) {
    this.value = value;
  }

    public String getValue() {
        return value;
    }
}
