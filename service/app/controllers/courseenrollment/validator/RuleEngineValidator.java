package controllers.courseenrollment.validator;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.models.util.LoggerUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    public boolean evaluateRules(Map<String, String> userAttributes, List<UserGroup> rules) {
        try {
            ObjectMapper om = new ObjectMapper();
            logger.info(null, "RuleEngineValidator::evaluateRules... rules: " + om.writeValueAsString(rules) 
                + ", userAttributes: " + om.writeValueAsString(userAttributes));
        } catch(Exception e) {
            logger.info(null,"RuleEngineValidator::evaluateRules exception: ");
        }
        
        boolean isCourseAllowed = false;
        for (UserGroup rule : rules) {
            // let's treat that 
            boolean isRuleSuccess = true;
            logger.info(null, "Validating rule: " + rule.getUserGroupId());
            for (UserGroupCriteria criteria : rule.getUserGroupCriteriaList()) {
                logger.info(null, "Validating criteriaKey: " + criteria.getCriteriaKey() + ", with Value: " + criteria.getCriteriaValue());
                if (!criteria.evaluate(userAttributes)) {
                    // User is not passed this criteria, skip this and continue to next userGroup rule.
                    isRuleSuccess = false;
                    break;
                }
            }
            if (isRuleSuccess) {
                //We found one rule which user has passed all the criteria. Let's allow the user to enrol.
                isCourseAllowed = true;
                logger.info(null, String.format("User %s successfully passed the rule using id: %s", userAttributes.get(JsonKey.USER), rule.getUserGroupId()));
                break;
            }
            logger.info(null, "isRuleSuccess: " + isRuleSuccess + "is course allowed: " + isCourseAllowed);
        }
        return isCourseAllowed;
    }
}
