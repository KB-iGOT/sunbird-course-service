package org.sunbird.learner.actors.coursebatch.dao;

import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.RequestContext;
import org.sunbird.models.user.courses.UserCourses;
import org.sunbird.models.user.events.UserEvents;

import java.util.Map;

public interface UserEventsDao {

    /**
     * Get user events information.
     *
     * @param batchId,userId user events identifiers
     * @param requestContext
     * @return User events information
     */
    UserEvents read(RequestContext requestContext, String userId, String courseId, String batchId);

    Response insertV2(RequestContext requestContext, Map<String, Object> userCoursesDetails);

    Response updateV2(RequestContext requestContext, String userId, String courseId, String batchId, Map<String, Object> updateAttributes);

}
