package org.sunbird.learner.actors.eventbatch;

import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.RequestContext;
import org.sunbird.models.course.batch.CourseBatch;
import org.sunbird.models.event.batch.EventBatch;
;

public interface EventBatchDao {

    /**
     * Read course batch for given identifier.
     *
     * @param eventBatchId Course batch identifier
     * @return Course batch information
     */
    EventBatch readById(String eventId, String batchId, RequestContext requestContext);

    /**
     * Create course batch.
     *
     * @param requestContext
     * @param courseBatch Course batch information to be created
     * @return Response containing identifier of created course batch
     */
    Response create(RequestContext requestContext, EventBatch courseBatch);

}
