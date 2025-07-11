package org.sunbird.enrolments

import akka.actor.ActorRef
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.sunbird.cache.util.RedisCacheUtil
import org.sunbird.common.CassandraUtil
import org.sunbird.common.exception.ProjectCommonException
import org.sunbird.common.models.util.ProjectUtil.{EnrolmentType, getConfigValue}
import org.sunbird.common.models.util._
import org.sunbird.common.request.{Request, RequestContext}
import org.sunbird.common.responsecode.ResponseCode
import org.sunbird.learner.actors.coursebatch.dao.impl.{BatchUserDaoImpl, CourseBatchDaoImpl, UserCoursesDaoImpl}
import org.sunbird.learner.actors.coursebatch.dao.{BatchUserDao, CourseBatchDao, UserCoursesDao}
import org.sunbird.learner.util.{ContentCacheHandlerV2, ContentUtil, JsonUtil, Util}
import org.sunbird.models.batch.user.BatchUser
import org.sunbird.models.course.batch.CourseBatch
import org.sunbird.models.user.courses.UserCourses
import org.sunbird.telemetry.util.TelemetryUtil

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util
import java.util.Date
import javax.inject.{Inject, Named}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.asScalaBufferConverter

class ExtendedCourseEnrollmentActor @Inject()(@Named("course-batch-notification-actor") courseBatchNotificationActorRef: ActorRef)(implicit val cacheUtil: RedisCacheUtil)
  extends BaseEnrolmentActor {

  var courseBatchDao: CourseBatchDao = new CourseBatchDaoImpl()
  var userCoursesDao: UserCoursesDao = new UserCoursesDaoImpl()
  var batchUserDao: BatchUserDao = new BatchUserDaoImpl()
  private val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

  override def onReceive(request: Request): Unit = {
    Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass.getName)

    request.getOperation match {
      case "enrollV2" => enroll(request)
      case _ => ProjectCommonException.throwClientErrorException(ResponseCode.invalidRequestData,
        ResponseCode.invalidRequestData.getErrorMessage)
    }
  }

  def enroll(request: Request): Unit = {
    val courseId = request.get(JsonKey.COURSE_ID).asInstanceOf[String]
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    val batchId = request.get(JsonKey.BATCH_ID).asInstanceOf[String]
    val recentLangOpt = Option(request.getContext.get(JsonKey.RECENT_LANGUAGE)).map(_.toString.toLowerCase)

    logger.info(request.getRequestContext, s"ExtendedCourseEnrolmentActor :: enrollWithLanguage :: Request received for courseId=$courseId, userId=$userId, batchId=$batchId, recentLanguage=$recentLangOpt")

    val fieldList = List(JsonKey.PRIMARYCATEGORY, JsonKey.IDENTIFIER, JsonKey.BATCHES)
    val contentData = getContentReadAPIData(courseId, fieldList, request)
    logger.info(request.getRequestContext,
      s"Content metadata fetched | contentDataEmpty=${contentData.isEmpty}")

    if (contentData.isEmpty || !util.Arrays.asList(getConfigValue(JsonKey.COURSE_ENROLL_ALLOWED_PRIMARY_CATEGORY).split(","): _*).contains(contentData.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String]))
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, courseId);

    // Validate batch access
    val batchData: CourseBatch = courseBatchDao.readById(courseId, batchId, request.getRequestContext)
    var enrolmentData: util.List[UserCourses] = userCoursesDao.readV2(request.getRequestContext, userId, courseId)
    if (CollectionUtils.isEmpty(enrolmentData)) enrolmentData = new util.ArrayList[UserCourses]()

    val batchUserData: BatchUser = batchUserDao.read(request.getRequestContext, batchId, userId)
    validateEnrolmentV3(batchData, enrolmentData, true)

    val dataBatch = createBatchUserMapping(batchId, userId, batchUserData)
    val existingEnrolmentForTheBatch = enrolmentData.asScala.find(_.getBatchId == batchId).orNull

    val data: java.util.Map[String, AnyRef] = createUserEnrolmentMap(userId, courseId, batchId, existingEnrolmentForTheBatch, request.getContext.getOrDefault(JsonKey.REQUEST_ID, "").asInstanceOf[String], request.getRequestContext)

    // set recent_language
    recentLangOpt.foreach(lang => data.put(JsonKey.RECENT_LANGUAGE, lang))

    val hasAccess = ContentUtil.getContentRead(courseId, request.getContext.getOrDefault(JsonKey.HEADER, new util.HashMap[String, String]).asInstanceOf[util.Map[String, String]])
    if (hasAccess) {
      upsertEnrollment(userId, courseId, batchId, data, dataBatch, existingEnrolmentForTheBatch == null, request.getRequestContext)
      cacheUtil.delete(getCacheKey(userId))
      sender().tell(successResponse(), self)
      logger.info(request.getRequestContext,
        s"Enrollment successful | courseId=$courseId, batchId=$batchId, userId=$userId")

      // Telemetry and notification
      generateTelemetryAudit(userId, courseId, batchId, data, "enrol", JsonKey.CREATE, request.getContext)
      notifyUser(userId, batchData, JsonKey.ADD)
    } else {
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, courseId)
    }
  }

  def getContentReadAPIData(programId: String, fieldList: List[String], request: Request): util.Map[String, AnyRef] = {
    val responseString: String = cacheUtil.get(programId)
    val contentData: util.Map[String, AnyRef] = if (StringUtils.isNotBlank(responseString)) {
      JsonUtil.deserialize(responseString, new util.HashMap[String, AnyRef]().getClass)
    } else {
      ContentCacheHandlerV2.getInstance().getContent(programId)
    }
    contentData
  }

  def validateEnrolmentV3(batchData: CourseBatch, enrolmentData: util.List[UserCourses], isEnrol: Boolean, isBlendedProgram: Boolean = false): Unit = {
    if (batchData == null)
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidCourseBatchId, ResponseCode.invalidCourseBatchId.getErrorMessage)

    if (!(EnrolmentType.inviteOnly.getVal.equalsIgnoreCase(batchData.getEnrollmentType) ||
      EnrolmentType.open.getVal.equalsIgnoreCase(batchData.getEnrollmentType)))
      ProjectCommonException.throwClientErrorException(ResponseCode.enrollmentTypeValidation, ResponseCode.enrollmentTypeValidation.getErrorMessage)

    if ((batchData.getStatus == 2) || (batchData.getEndDate != null && LocalDateTime.now().isAfter(LocalDate.parse(DATE_FORMAT.format(batchData.getEndDate), DateTimeFormatter.ofPattern("yyyy-MM-dd")).atTime(LocalTime.MAX))))
      ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchAlreadyCompleted, ResponseCode.courseBatchAlreadyCompleted.getErrorMessage)

    if (isBlendedProgram) {
      if (isEnrol && batchData.getStartDate != null && LocalDateTime.now().isAfter(LocalDate.parse(DATE_FORMAT.format(batchData.getStartDate), DateTimeFormatter.ofPattern("yyyy-MM-dd")).atTime(LocalTime.MAX)))
        ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchAlreadyStarted, ResponseCode.courseBatchAlreadyStarted.getErrorMessage)
    } else {
      if (isEnrol && batchData.getEnrollmentEndDate != null && LocalDateTime.now().isAfter(LocalDate.parse(DATE_FORMAT.format(batchData.getEnrollmentEndDate), DateTimeFormatter.ofPattern("yyyy-MM-dd")).atTime(LocalTime.MAX)))
        ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchEnrollmentDateEnded, ResponseCode.courseBatchEnrollmentDateEnded.getErrorMessage)
    }

    // If enrolling, check if any active enrollment already exists
    if (isEnrol && enrolmentData.nonEmpty) {
      enrolmentData.find(_.isActive) match {
        case Some(enrolment) if enrolment.getBatchId == batchData.getBatchId =>
          // User is already enrolled in the same batch
          ProjectCommonException.throwClientErrorException(ResponseCode.userAlreadyEnrolledCourse, ResponseCode.userAlreadyEnrolledCourse.getErrorMessage)
        case Some(_) =>
          // User is already enrolled in a different batch
          ProjectCommonException.throwClientErrorException(ResponseCode.userAlreadyEnrolledCourseWithDifferentBatch, ResponseCode.userAlreadyEnrolledCourseWithDifferentBatch.getErrorMessage)
        case None => // No active enrollment found, continue processing
      }
    }

    // If unenrolling, check if the user is NOT enrolled in any active batch
    if (!isEnrol && enrolmentData.forall(e => e == null || !e.isActive))
      ProjectCommonException.throwClientErrorException(ResponseCode.userNotEnrolledCourse, ResponseCode.userNotEnrolledCourse.getErrorMessage)

    // If unenrolling, check if the user has already completed the course
    if (!isEnrol && enrolmentData.exists(_.getStatus == ProjectUtil.ProgressStatus.COMPLETED.getValue))
      ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchAlreadyCompleted, ResponseCode.courseBatchAlreadyCompleted.getErrorMessage)
  }

  def createBatchUserMapping(batchId: String, userId: String, batchUserData: BatchUser): java.util.Map[String, AnyRef] =
    new java.util.HashMap[String, AnyRef]() {
      put(JsonKey.BATCH_ID, batchId)
      put(JsonKey.USER_ID, userId)
      put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue.asInstanceOf[AnyRef])
      if (batchUserData == null) {
        put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getTimeStamp)
      } else {
        put(JsonKey.COURSE_ENROLL_DATE, batchUserData.getEnrolledDate)
      }
    }

  def createUserEnrolmentMap(userId: String, courseId: String, batchId: String, enrolmentData: UserCourses, requestedBy: String, requestContext: RequestContext): java.util.Map[String, AnyRef] =
    new java.util.HashMap[String, AnyRef]() {
      {
        put(JsonKey.USER_ID, userId)
        put(JsonKey.COURSE_ID, courseId)
        put(JsonKey.BATCH_ID, batchId)
        put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue.asInstanceOf[AnyRef])
        if (null == enrolmentData) {
          put(JsonKey.ADDED_BY, requestedBy)
          put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getTimeStamp)
          put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue.asInstanceOf[AnyRef])
          put(JsonKey.DATE_TIME, new Timestamp(new Date().getTime))
          put(JsonKey.COURSE_PROGRESS, 0.asInstanceOf[AnyRef])
        } else {
          logger.info(requestContext, "user-enrollment-null-tag, userId : " + userId + " courseId : " + courseId + " batchId : " + batchId + enrolmentData.toString);
        }
      }
    }

  def upsertEnrollment(userId: String, courseId: String, batchId: String, data: java.util.Map[String, AnyRef], dataBatch: java.util.Map[String, AnyRef], isNew: Boolean, requestContext: RequestContext): Unit = {
    val dataMap = CassandraUtil.changeCassandraColumnMapping(data)
    val dataBatchMap = CassandraUtil.changeCassandraColumnMapping(dataBatch)

    try {
      val activeStatus = dataMap.get(JsonKey.ACTIVE);
      logger.info(requestContext, "upsertEnrollment :: IsNew :: " + isNew + " ActiveStatus :: " + activeStatus + " DataMap is :: " + dataMap + " DataBatchMap:: " + dataBatchMap)
      if (activeStatus == null) {
        throw new Exception("Active Value is null in upsertEnrollment");
      }
    } catch {
      case e: Exception =>
        logger.error(requestContext, "Exception in upsertEnrollment list : user ::" + userId + "| Exception is:" + e.getMessage, e)
        throw e;
    }
    // END
    if (isNew) {
      userCoursesDao.insertExtendedEnrollmentV2(requestContext, dataMap)
      batchUserDao.insertBatchLookupRecord(requestContext, dataBatchMap)
    } else {
      userCoursesDao.updateExtendedEnrollV2(requestContext, userId, courseId, batchId, dataMap)
      batchUserDao.updateBatchLookupRecord(requestContext, batchId, userId, dataBatchMap, dataMap)
    }
  }

  def getCacheKey(userId: String) = s"$userId:user-enrolments"


  def generateTelemetryAudit(userId: String, courseId: String, batchId: String, data: java.util.Map[String, AnyRef], correlation: String, state: String, context: java.util.Map[String, AnyRef]): Unit = {
    val contextMap = new java.util.HashMap[String, AnyRef]()
    contextMap.putAll(context)
    contextMap.put(JsonKey.ACTOR_ID, userId)
    contextMap.put(JsonKey.ACTOR_TYPE, "User")
    val targetedObject = TelemetryUtil.generateTargetObject(userId, JsonKey.USER, state, null)
    targetedObject.put(JsonKey.ROLLUP, new java.util.HashMap[String, AnyRef]() {
      {
        put("l1", courseId)
      }
    })
    val correlationObject = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
    TelemetryUtil.generateCorrelatedObject(courseId, JsonKey.COURSE, correlation, correlationObject)
    TelemetryUtil.generateCorrelatedObject(batchId, TelemetryEnvKey.BATCH, "user.batch", correlationObject)
    val request = new java.util.HashMap[String, AnyRef]()
    request.put(JsonKey.USER_ID, userId)
    request.put(JsonKey.COURSE_ID, courseId)
    request.put(JsonKey.BATCH_ID, batchId)
    request.put(JsonKey.COURSE_ENROLL_DATE, data.get(JsonKey.COURSE_ENROLL_DATE))
    request.put(JsonKey.ACTIVE, data.get(JsonKey.ACTIVE))
    TelemetryUtil.telemetryProcessingCall(request, targetedObject, correlationObject, contextMap, "enrol")
  }

  def notifyUser(userId: String, batchData: CourseBatch, operationType: String): Unit = {
    val isNotifyUser = java.lang.Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_COURSE_BATCH_NOTIFICATIONS_ENABLED))
    if (isNotifyUser) {
      val request = new Request()
      request.setOperation(ActorOperations.COURSE_BATCH_NOTIFICATION.getValue)
      request.put(JsonKey.USER_ID, userId)
      request.put(JsonKey.COURSE_BATCH, batchData)
      request.put(JsonKey.OPERATION_TYPE, operationType)
      courseBatchNotificationActorRef.tell(request, getSelf())
    }
  }
}
