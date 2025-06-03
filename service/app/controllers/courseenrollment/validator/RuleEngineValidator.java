package controllers.courseenrollment.validator;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.models.util.LoggerUtil;

import java.util.List;
import java.util.Map;
import org.sunbird.learner.actors.accesssettings.model.UserGroup;
import org.sunbird.learner.actors.accesssettings.model.UserGroupCriteria;

/**
 * This class is responsible for validating requests related to the Rule Engine.
 * It checks for mandatory parameters are exist for user profile against the rule.
 */
public class RuleEngineValidator {
    private LoggerUtil logger = new LoggerUtil(RuleEngineValidator.class);
    private static RuleEngineValidator instance;

    private RuleEngineValidator() {
        // Private constructor to prevent instantiation
    }

    public static RuleEngineValidator getInstance() {
        if (instance == null) {
            synchronized (RuleEngineValidator.class) {
                if (instance == null) {
                    instance = new RuleEngineValidator();
                }
            }
            instance = new RuleEngineValidator();
        }
        return instance;
    }

    public String evaluateRules(Map<String, String> userAttributes, List<UserGroup> rules) {
        String errMsg = "";
        for (UserGroup rule : rules) {
            for (UserGroupCriteria criteria : rule.getUserGroupCriteriaList()) {
                if (!criteria.evaluate(userAttributes)) {
                    errMsg = String.format("User does not meet '%s' criteria.", criteria.getCriteriaKey());
                    logger.info(null, "Rule failed for user: " + userAttributes.get(JsonKey.USER_ID) +
                                " with criteria: " + criteria.getCriteriaKey() + " for rule: " + rule.getUserGroupId());
                    return errMsg;
                }
            }
        }
        return errMsg;
    }
}
