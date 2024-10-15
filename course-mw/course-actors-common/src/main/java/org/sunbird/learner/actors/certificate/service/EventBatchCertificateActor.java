package org.sunbird.learner.actors.certificate.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.base.BaseActor;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestContext;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.constants.CourseJsonKey;
import org.sunbird.learner.util.CourseBatchUtil;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.sunbird.learner.actors.eventbatch.EventBatchDao;
import org.sunbird.learner.actors.eventbatch.impl.EventBatchDaoImpl;
import org.sunbird.models.event.batch.EventBatch;


public class EventBatchCertificateActor extends BaseActor {

    private EventBatchDao eventBatchDao = new EventBatchDaoImpl();
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, TelemetryEnvKey.USER, this.getClass().getName());

        String requestedOperation = request.getOperation();
        switch (requestedOperation) {
            case "addCertificateToEventBatch":
                addCertificateTemplateToCourseBatch(request);
                break;
            case "removeCertificateFromEventBatch":
                removeCertificateTemplateFromCourseBatch(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    private void addCertificateTemplateToCourseBatch(Request request) {
        Map<String, Object> batchRequest = (Map<String, Object>) request.getRequest().get(JsonKey.BATCH);
        final String batchId = (String) batchRequest.get(JsonKey.BATCH_ID);
        final String eventId = (String) batchRequest.get(JsonKey.EVENT_ID);
        EventBatch exsistingBatch = eventBatchDao.readById(eventId, batchId, request.getRequestContext());
        if (exsistingBatch == null) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.CLIENT_ERROR, "No such batchId exists");
        }
        Map<String, Object> template = (Map<String, Object>) batchRequest.get(CourseJsonKey.TEMPLATE);
        String templateId = (String) template.get(JsonKey.IDENTIFIER);
        validateTemplateDetails(request.getRequestContext(), templateId, template);
        logger.info(request.getRequestContext(), "Validated certificate template to batchID: " + batchId);
        eventBatchDao.addCertificateTemplateToEventBatch(request.getRequestContext(), eventId, batchId, templateId,
                template);
        logger.info(request.getRequestContext(), "Added certificate template to batchID: " + batchId);
        Map<String, Object> eventBatch = mapESFieldsToObject(CassandraUtil.convertStringToMap(
                eventBatchDao.getEventBatch(request.getRequestContext(), eventId, batchId), "batchAttributes"));
        CourseBatchUtil.syncCourseBatchForeground(request.getRequestContext(), batchId, eventBatch);
        logger.info(request.getRequestContext(), "Synced to es certificate template to batchID: " + batchId);
        Response response = new Response();
        response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
        sender().tell(response, self());
    }

    private void removeCertificateTemplateFromCourseBatch(Request request) {
        Map<String, Object> batchRequest = (Map<String, Object>) request.getRequest().get(JsonKey.BATCH);
        final String batchId = (String) batchRequest.get(JsonKey.BATCH_ID);
        final String eventId = (String) batchRequest.get(JsonKey.COURSE_ID);
        CourseBatchUtil.validateCourseBatch(request.getRequestContext(), eventId, batchId);
        Map<String, Object> template = (Map<String, Object>) batchRequest.get(CourseJsonKey.TEMPLATE);
        String templateId = (String) template.get(JsonKey.IDENTIFIER);
        CourseBatchUtil.validateTemplate(request.getRequestContext(), templateId);
        eventBatchDao.removeCertificateTemplateFromEventBatch(request.getRequestContext(), eventId, batchId,
                templateId);
        Map<String, Object> courseBatch = mapESFieldsToObject(
                eventBatchDao.getEventBatch(request.getRequestContext(), eventId, batchId));
        CourseBatchUtil.syncCourseBatchForeground(request.getRequestContext(), batchId, courseBatch);
        Response response = new Response();
        response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
        sender().tell(response, self());
    }

    private void validateTemplateDetails(RequestContext requestContext, String templateId,
            Map<String, Object> template) {
        Map<String, Object> templateDetails = CourseBatchUtil.validateTemplate(requestContext, templateId);
        try {
            if ((!templateDetails.containsKey(CourseJsonKey.ISSUER)
                    || !templateDetails.containsKey(CourseJsonKey.SIGNATORY_LIST))
                    && (!template.containsKey(CourseJsonKey.ISSUER)
                            || !template.containsKey(CourseJsonKey.SIGNATORY_LIST))) {
                ProjectCommonException.throwClientErrorException(
                        ResponseCode.CLIENT_ERROR,
                        "Issuer or signatoryList is empty. Invalid template Id: " + templateId);
            }
            Map<String, Object> templateData = (Map<String, Object>) templateDetails.getOrDefault(JsonKey.DATA,
                    new HashMap<>());
            String certName = (String) templateData.getOrDefault(JsonKey.TITLE,
                    (String) templateDetails.getOrDefault(JsonKey.NAME, ""));

            template.put(JsonKey.NAME, certName);
            template.put(JsonKey.URL, templateDetails.getOrDefault("artifactUrl", ""));
            template.put(JsonKey.CRITERIA, mapper.writeValueAsString(template.get(JsonKey.CRITERIA)));
            if (null != template.get(CourseJsonKey.ISSUER)) {
                template.put(
                        CourseJsonKey.ISSUER, mapper.writeValueAsString(template.get(CourseJsonKey.ISSUER)));
            } else {
                template.put(
                        CourseJsonKey.ISSUER, mapper.writeValueAsString(templateDetails.get(CourseJsonKey.ISSUER)));
            }
            if (null != template.get(CourseJsonKey.SIGNATORY_LIST)) {
                template.put(
                        CourseJsonKey.SIGNATORY_LIST,
                        mapper.writeValueAsString(template.get(CourseJsonKey.SIGNATORY_LIST)));
            } else {
                template.put(
                        CourseJsonKey.ISSUER,
                        mapper.writeValueAsString(templateDetails.get(CourseJsonKey.SIGNATORY_LIST)));
            }
            if (MapUtils.isNotEmpty((Map<String, Object>) template.get(CourseJsonKey.NOTIFY_TEMPLATE))) {
                template.put(
                        CourseJsonKey.NOTIFY_TEMPLATE,
                        mapper.writeValueAsString(template.get(CourseJsonKey.NOTIFY_TEMPLATE)));
            }
            if (MapUtils.isNotEmpty((Map<String, Object>) template.get(CourseJsonKey.ADDITIONAL_PROPS))) {
                template.put(
                        CourseJsonKey.ADDITIONAL_PROPS,
                        mapper.writeValueAsString(template.get(CourseJsonKey.ADDITIONAL_PROPS)));
            }
        } catch (JsonProcessingException ex) {
            ProjectCommonException.throwClientErrorException(
                    ResponseCode.invalidData,
                    "Error in parsing certificate template data, Please check fields data and dataTypes");
        }
    }

    private Map<String, Object> mapESFieldsToObject(Map<String, Object> eventBatch) {
        Map<String, Map<String, Object>> certificateTemplates = (Map<String, Map<String, Object>>) eventBatch
                .getOrDefault(CourseJsonKey.CERT_TEMPLATES, null);
        if (MapUtils.isNotEmpty(certificateTemplates)) {
            certificateTemplates
                    .entrySet()
                    .stream()
                    .forEach(
                            cert_template -> certificateTemplates.put(
                                    cert_template.getKey(), mapToObject(cert_template.getValue())));
            eventBatch.put(CourseJsonKey.CERTIFICATE_TEMPLATES_COLUMN, certificateTemplates);
        }
        return eventBatch;
    }

    private Map<String, Object> mapToObject(Map<String, Object> template) {
        try {
            template.put(
                    JsonKey.CRITERIA,
                    mapper.readValue(
                            (String) template.get(JsonKey.CRITERIA),
                            new TypeReference<HashMap<String, Object>>() {
                            }));
            if (StringUtils.isNotEmpty((String) template.get(CourseJsonKey.SIGNATORY_LIST))) {
                template.put(
                        CourseJsonKey.SIGNATORY_LIST,
                        mapper.readValue(
                                (String) template.get(CourseJsonKey.SIGNATORY_LIST),
                                new TypeReference<List<Object>>() {
                                }));
            }
            if (StringUtils.isNotEmpty((String) template.get(CourseJsonKey.ISSUER))) {
                template.put(
                        CourseJsonKey.ISSUER,
                        mapper.readValue(
                                (String) template.get(CourseJsonKey.ISSUER),
                                new TypeReference<HashMap<String, Object>>() {
                                }));
            }
            if (StringUtils.isNotEmpty((String) template.get(CourseJsonKey.NOTIFY_TEMPLATE))) {
                template.put(
                        CourseJsonKey.NOTIFY_TEMPLATE,
                        mapper.readValue(
                                (String) template.get(CourseJsonKey.NOTIFY_TEMPLATE),
                                new TypeReference<HashMap<String, Object>>() {
                                }));
            }
            if (StringUtils.isNotEmpty((String) template.get(CourseJsonKey.ADDITIONAL_PROPS))) {
                template.put(
                        CourseJsonKey.ADDITIONAL_PROPS,
                        mapper.readValue(
                                (String) template.get(CourseJsonKey.ADDITIONAL_PROPS),
                                new TypeReference<HashMap<String, Object>>() {
                                }));
            }
        } catch (Exception ex) {
            logger.error(null, "EventBatchCertificateActor:mapToObject Exception occurred with error message ==", ex);
        }
        return template;
    }
}
