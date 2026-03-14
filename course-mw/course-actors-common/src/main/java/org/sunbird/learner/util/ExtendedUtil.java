package org.sunbird.learner.util;

import org.sunbird.common.models.util.JsonKey;

import java.util.HashMap;
import java.util.Map;


public class ExtendedUtil {

    public static final String LEARNER_COURSE_DB = "learnerCourse_db";
    public static final String LEARNER_CONTENT_DB = "learnerContent_db";
    public static final String COURSE_KEY_SPACE_NAME = "sunbird_courses";
    public static final String USER_ENROLMENTS_V2_DB = "user_enrolments_v2";
    public static final String USER_BADGE_LOOKUP_DB = "userBadgeLookup_db";

    public static final Map<String, DbInfo> dbInfoMap = new HashMap<>();

    static {
        try {
            initializeDBProperty();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error initializing DB properties in ExtendedUtil", e);
        }
    }



    /** This method will initialize the cassandra data base property */
    private static void initializeDBProperty() {
        dbInfoMap.put(
                LEARNER_COURSE_DB, getDbInfoObject(COURSE_KEY_SPACE_NAME, "user_enrolments_v2"));
        dbInfoMap.put(
                LEARNER_CONTENT_DB, getDbInfoObject(COURSE_KEY_SPACE_NAME, "user_content_consumption_v2"));
        dbInfoMap.put(
                USER_ENROLMENTS_V2_DB, getDbInfoObject(COURSE_KEY_SPACE_NAME, "user_enrolments_v2"));
        dbInfoMap.put(
                USER_ENROLMENTS_V2_DB, getDbInfoObject(COURSE_KEY_SPACE_NAME, "user_enrolments_v2"));
        dbInfoMap.put(
                JsonKey.EXTERNAL_TRAINING_ENROLLMENT_BATCH_DB, getDbInfoObject(COURSE_KEY_SPACE_NAME, "external_training_enrolments_batch_lookup"));
        dbInfoMap.put(
                USER_BADGE_LOOKUP_DB, getDbInfoObject(COURSE_KEY_SPACE_NAME, "user_badge_lookup"));
    }

    private static DbInfo getDbInfoObject(String keySpace, String table) {

        DbInfo dbInfo = new DbInfo();

        dbInfo.setKeySpace(keySpace);
        dbInfo.setTableName(table);

        return dbInfo;
    }

    public static class DbInfo {
        private String keySpace;
        private String tableName;
        private String userName;
        private String password;
        private String ip;
        private String port;

        /**
         * @param keySpace
         * @param tableName
         * @param userName
         * @param password
         */
        DbInfo(
                String keySpace,
                String tableName,
                String userName,
                String password,
                String ip,
                String port) {
            this.keySpace = keySpace;
            this.tableName = tableName;
            this.userName = userName;
            this.password = password;
            this.ip = ip;
            this.port = port;
        }

        /** No-arg constructor */
        DbInfo() {}

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Util.DbInfo) {
                Util.DbInfo ob = (Util.DbInfo) obj;
                if (this.ip.equals(ob.getIp())
                        && this.port.equals(ob.getPort())
                        && this.keySpace.equals(ob.getKeySpace())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 1;
        }

        public String getKeySpace() {
            return keySpace;
        }

        public void setKeySpace(String keySpace) {
            this.keySpace = keySpace;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }
    }
}
