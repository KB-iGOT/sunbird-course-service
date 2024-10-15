package org.sunbird.learner.actors.eventbatch;

import java.util.Map;

import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.RequestContext;
import org.sunbird.models.event.batch.EventBatch;

public interface EventBatchDao {

    /**
     * Read event batch for given identifier.
     *
     * @param eventBatchId Event batch identifier
     * @return Event batch information
     */
    EventBatch readById(String eventId, String batchId, RequestContext requestContext);

    /**
     * Create course batch.
     *
     * @param requestContext
     * @param eventBatch    Event batch information to be created
     * @return Response containing identifier of created course batch
     */
    Response create(RequestContext requestContext, EventBatch courseBatch);

    public void addCertificateTemplateToEventBatch(
            RequestContext requestContext, String eventId, String batchId, String templateId,
            Map<String, Object> templateDetails);

    public void removeCertificateTemplateFromEventBatch(
            RequestContext requestContext, String eventId, String batchId, String templateId);

    public Map<String, Object> getEventBatch(RequestContext requestContext, String eventId, String batchId);

}
