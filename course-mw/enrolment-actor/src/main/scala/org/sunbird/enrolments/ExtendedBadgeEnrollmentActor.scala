package org.sunbird.enrolments

import akka.actor.ActorRef
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.collections4.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.sunbird.cache.util.RedisCacheUtil
import org.sunbird.common.exception.ProjectCommonException
import org.sunbird.common.models.response.Response
import org.sunbird.common.models.util.ProjectUtil.{EnrolmentType, getConfigValue}
import org.sunbird.common.models.util._
import org.sunbird.common.request.{Request, RequestContext}
import org.sunbird.common.responsecode.ResponseCode
import org.sunbird.common.{CassandraUtil, Constants}
import org.sunbird.helper.ServiceFactory
import org.sunbird.kafka.client.{InstructionEventGenerator, KafkaClient}
import org.sunbird.learner.actors.course.dao.impl.ContentHierarchyDaoImpl
import org.sunbird.learner.actors.coursebatch.dao.impl.{BatchUserDaoImpl, CourseBatchDaoImpl, UserCoursesDaoImpl}
import org.sunbird.learner.actors.coursebatch.dao.{BatchUserDao, CourseBatchDao, UserCoursesDao}
import org.sunbird.learner.util._
import org.sunbird.models.batch.user.BatchUser
import org.sunbird.models.course.batch.CourseBatch
import org.sunbird.models.user.courses.UserCourses
import org.sunbird.telemetry.util.TelemetryUtil

import java.sql.Timestamp
import java.text.{MessageFormat, SimpleDateFormat}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util
import java.util.{Calendar, Date, TimeZone, UUID}
import javax.inject.{Inject, Named}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class ExtendedBadgeEnrollmentActor @Inject()(@Named("course-batch-notification-actor") courseBatchNotificationActorRef: ActorRef)(implicit val cacheUtil: RedisCacheUtil)
  extends BaseEnrolmentActor {

  var courseBatchDao: CourseBatchDao = new CourseBatchDaoImpl()
  var userCoursesDao: UserCoursesDao = new UserCoursesDaoImpl()
  var batchUserDao: BatchUserDao = new BatchUserDaoImpl()
  private val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  var contentHierarchyDao: ContentHierarchyDaoImpl = new ContentHierarchyDaoImpl()
  private val pageDbInfo = Util.dbInfoMap.get(JsonKey.USER_KARMA_POINTS_DB)
  var isRetiredCoursesIncludedInEnrolList = false
  val statusMap: Map[String, Int] = Map("In-Progress" -> 1, "Completed" -> 2, "Not-Started" -> 0)
  val redisCollectionIndex = if (StringUtils.isNotBlank(ProjectUtil.getConfigValue("redis_collection_index")))
    (ProjectUtil.getConfigValue("redis_collection_index")).toInt else 10
  private val externalCourseEnrolDbInfo = Util.dbInfoMap.get(JsonKey.EXTERNAL_COURSES_ENROLMENT_DB)
  private val cassandraOperation = ServiceFactory.getInstance
  val jsonFields = Set[String]("lrcProgressDetails")
  private val mapper = new ObjectMapper
  private val courseAllowedPrimaryCategories: java.util.List[String] =
    java.util.Arrays.asList(getConfigValue(JsonKey.COURSE_ENROLL_ALLOWED_PRIMARY_CATEGORY).split(","): _*)
  private val programAllowedPrimaryCategories: Set[String] =
    getConfigValue(JsonKey.PROGRAM_ENROLL_ALLOWED_PRIMARY_CATEGORY).split(",").toSet

  private val adminAllowedPrimaryCategories: Set[String] =
    getConfigValue(JsonKey.ADMIN_PROGRAM_ENROLL_ALLOWED_PRIMARY_CATEGORY).split(",").toSet
  private val enrolmentDBInfo = ExtendedUtil.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB)
  private val consumptionDBInfo = ExtendedUtil.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB)
  private val assessmentAggregatorDBInfo = Util.dbInfoMap.get(JsonKey.ASSESSMENT_AGGREGATOR_DB)
  private val badgeDbInfo = ExtendedUtil.dbInfoMap.get(ExtendedUtil.USER_BADGE_LOOKUP_DB)
  val dateFormatter = ProjectUtil.getDateFormatter

  dateFormatter.setTimeZone(
    TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)))

  override def preStart { println("Starting ExtendedBadgeEnrollmentActor") }

  override def postStop {
    cacheUtil.closePool()
    println("ExtendedBadgeEnrollmentActor stopped successfully")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println(s"Restarting ExtendedBadgeEnrollmentActor: $message")
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

  override def onReceive(request: Request): Unit = {
    Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass.getName)

    request.getOperation match {
      case "list" => list(request)
      case _ => ProjectCommonException.throwClientErrorException(ResponseCode.invalidRequestData,
        ResponseCode.invalidRequestData.getErrorMessage)
    }
  }

  def getContentReadAPIData(programId: String, fieldList: List[String], request: Request): util.Map[String, AnyRef] = {
    val responseString: String = cacheUtil.get(programId)
    val contentData: util.Map[String, AnyRef] = if (StringUtils.isNotBlank(responseString)) {
      JsonUtil.deserialize(responseString, new util.HashMap[String, AnyRef]().getClass)
    } else {
      ContentCacheHandlerV2.getInstance().getContent(programId)
    }
    if (contentData == null || contentData.isEmpty) {
      throw new ProjectCommonException(
        ResponseCode.invalidCourseId.getErrorCode,
        "Content not found for id: " + programId,
        ResponseCode.RESOURCE_NOT_FOUND.getResponseCode
      )
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

  def createUserEnrolmentMap(userId: String, courseId: String, batchId: String, enrolmentData: UserCourses, requestedBy: String, requestContext: RequestContext, recentLanguage: String): java.util.Map[String, AnyRef] =
    new java.util.HashMap[String, AnyRef]() {
      {
        put(JsonKey.USER_ID, userId)
        put(JsonKey.COURSE_ID, courseId)
        put(JsonKey.BATCH_ID, batchId)
        put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue.asInstanceOf[AnyRef])
        put(JsonKey.RECENT_LANGUAGE,recentLanguage)
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

  def notifyUser(userId: String, batchData: CourseBatch, operationType: String, recentLanguage: String): Unit = {
    val isNotifyUser = java.lang.Boolean.parseBoolean(PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_COURSE_BATCH_NOTIFICATIONS_ENABLED))
    if (isNotifyUser) {
      val request = new Request()
      request.setOperation(ActorOperations.COURSE_BATCH_NOTIFICATION.getValue)
      request.put(JsonKey.USER_ID, userId)
      request.put(JsonKey.COURSE_BATCH, batchData)
      request.put(JsonKey.OPERATION_TYPE, operationType)
      request.put(JsonKey.RECENT_LANGUAGE, recentLanguage)
      courseBatchNotificationActorRef.tell(request, getSelf())
    }
  }

  def list(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    logger.info(request.getRequestContext, "ExtendedBadgeEnrollmentActor :: list :: UserId = " + userId)
    try {
      val statusFilter = Option(request.get(JsonKey.STATUS))
        .map(_.asInstanceOf[String])
        .orNull

      logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: list :: Status filter: $statusFilter")
      val savedStatus = request.get(JsonKey.STATUS)
      request.getRequest.remove(JsonKey.STATUS)

      // Fetch internal enrollments
      val activeEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getActiveEnrollments(userId, request)

      // Fetch external enrollments
      val externalEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getExternalEnrollments(userId, request)
      logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: list :: Found ${externalEnrolments.size()} external enrollments")

      if (savedStatus != null) {
        request.getRequest.put(JsonKey.STATUS, savedStatus)
      }

      // Process internal badge stats
      val badgeStats = getBadgeStats(request, userId, activeEnrolments, statusFilter, isExternal = false)

      // Process external badge stats
      val externalBadgeStats = getBadgeStats(request, userId, externalEnrolments, statusFilter, isExternal = true)

      // Merge summaries
      val internalSummary = badgeStats.get(JsonKey.SUMMARY).asInstanceOf[java.util.Map[String, AnyRef]]
      val externalSummary = externalBadgeStats.get(JsonKey.SUMMARY).asInstanceOf[java.util.Map[String, AnyRef]]

      val totalBadgesEarned = internalSummary.get(JsonKey.TOTAL_BADGES_EARNED).asInstanceOf[Integer] +
                              externalSummary.get(JsonKey.TOTAL_BADGES_EARNED).asInstanceOf[Integer]
      val totalCourseCompleted = internalSummary.get(JsonKey.COURSE_COMPLETED).asInstanceOf[Integer] +
                                 externalSummary.get(JsonKey.COURSE_COMPLETED).asInstanceOf[Integer]

      // Merge badge lists
      val mergedEarnedBadges = mergeBadgeLists(
        badgeStats.get(JsonKey.EARNED_BADGES_DETAILS).asInstanceOf[java.util.Map[String, AnyRef]],
        externalBadgeStats.get(JsonKey.EARNED_BADGES_DETAILS).asInstanceOf[java.util.Map[String, AnyRef]]
      )

      val mergedInProgressBadges = mergeBadgeLists(
        badgeStats.get(JsonKey.IN_PROGRESS_BADGES_DETAILS).asInstanceOf[java.util.Map[String, AnyRef]],
        externalBadgeStats.get(JsonKey.IN_PROGRESS_BADGES_DETAILS).asInstanceOf[java.util.Map[String, AnyRef]]
      )

      // Recalculate completionRate based on merged data
      val inProgressCount = mergedInProgressBadges.get(JsonKey.COUNT).asInstanceOf[Integer]
      val totalAttempted = totalBadgesEarned + inProgressCount
      val completionRate = if (totalAttempted > 0) {
        (totalBadgesEarned * 100) / totalAttempted
      } else {
        0
      }

      val mergedSummary = new java.util.HashMap[String, AnyRef]()
      mergedSummary.put(JsonKey.TOTAL_BADGES_EARNED, totalBadgesEarned.asInstanceOf[AnyRef])
      mergedSummary.put(JsonKey.COURSE_COMPLETED, totalCourseCompleted.asInstanceOf[AnyRef])
      mergedSummary.put(JsonKey.COMPLETION_RATE, completionRate.asInstanceOf[AnyRef])

      val response = new Response()
      response.put(JsonKey.SUMMARY, mergedSummary)
      // Add details based on status filter
      if ("Completed".equalsIgnoreCase(statusFilter)) {
        response.put(JsonKey.EARNED_BADGES_DETAILS, mergedEarnedBadges)
      } else if ("In-Progress".equalsIgnoreCase(statusFilter) || "InProgress".equalsIgnoreCase(statusFilter)) {
        response.put(JsonKey.IN_PROGRESS_BADGES_DETAILS, mergedInProgressBadges)
      } else {
        response.put(JsonKey.EARNED_BADGES_DETAILS, mergedEarnedBadges)
        response.put(JsonKey.IN_PROGRESS_BADGES_DETAILS, mergedInProgressBadges)
      }
      logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: list :: Badge stats computed with statusFilter=$statusFilter")
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : request ::" + mapper.writeValueAsString(request) + "| Exception is:" + e.getMessage, e)
        throw e
    }
  }

  def getActiveEnrollments(userId: String, request: Request): java.util.List[java.util.Map[String, AnyRef]] = {
    var enrolments: java.util.List[java.util.Map[String, AnyRef]] = new java.util.ArrayList()
    logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getActiveEnrollments :: Fetching enrollments for userId=$userId")
    enrolments = userCoursesDao.listEnrolments_v2(request.getRequestContext, userId, null);
    logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getActiveEnrollments :: Found ${enrolments.size()} total enrollments from DB")
    val status: Array[String] = request.get(JsonKey.STATUS) match {
      case arr: Array[String] => arr
      case list: java.util.List[String] => list.toArray(new Array[String](list.size()))
      case str: String => Array(str)
      case _ => null
    }

    val statusFilteredEnrolments = scala.collection.mutable.ArrayBuffer[java.util.List[java.util.Map[String, AnyRef]]]()

    if (CollectionUtils.isNotEmpty(enrolments)) {
      enrolments = enrolments.filter(e => e.getOrDefault(JsonKey.ACTIVE, false.asInstanceOf[AnyRef]).asInstanceOf[Boolean]).toList.asJava
      logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getActiveEnrollments :: After active filter: ${enrolments.size()} enrollments")
      if (status != null && status.nonEmpty && status.exists(s => statusMap.contains(s))) {
        for (statusValue <- status) {
          if (statusMap.get(statusValue).contains(1)) {
            statusFilteredEnrolments.append(
              enrolments
                .filter(e => e.getOrDefault(JsonKey.STATUS, (-1).asInstanceOf[AnyRef]).asInstanceOf[Integer] != 2)
                .toList
                .asJava
            )
          } else {
            statusFilteredEnrolments.append(
              enrolments
                .filter(e => e.getOrDefault(JsonKey.STATUS, (-1).asInstanceOf[AnyRef]).asInstanceOf[Integer] == 2)
                .toList
                .asJava
            )
          }
        }
      }
      enrolments = if (statusFilteredEnrolments.nonEmpty) {
        statusFilteredEnrolments.flatten.toList.asJava
      } else {
        enrolments
      }
      logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getActiveEnrollments :: After status filter: ${enrolments.size()} enrollments")
      
      // Log course IDs for debugging
      if (!enrolments.isEmpty) {
        val courseIds = enrolments.asScala.map(e => e.get(JsonKey.COURSE_ID)).mkString(", ")
        logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getActiveEnrollments :: Course IDs: $courseIds")
      }
      
      enrolments
    } else {
      logger.info(request.getRequestContext, "ExtendedBadgeEnrollmentActor :: getActiveEnrollments :: No enrollments found in DB")
      new util.ArrayList[java.util.Map[String, AnyRef]]()
    }
  }

  def updateProgressData(enrolments: java.util.List[java.util.Map[String, AnyRef]], requestContext: RequestContext): util.List[java.util.Map[String, AnyRef]] = {
    enrolments.map { enrolment =>
      val statusObj: Int = enrolment.getOrDefault("status", 0.asInstanceOf[AnyRef]).asInstanceOf[Int]
      if (statusObj.equals(2)) {
        enrolment.put("status", 2.asInstanceOf[AnyRef])
        enrolment.put("completionPercentage", 100.asInstanceOf[AnyRef])
      } else {
        val leafNodesCount: Int = enrolment.getOrDefault("leafNodesCount", 0.asInstanceOf[AnyRef]).asInstanceOf[Int]
        val progress: Int = enrolment.getOrDefault("progress", 0.asInstanceOf[AnyRef]).asInstanceOf[Int]
        enrolment.put("status", getCompletionStatus(progress, leafNodesCount).asInstanceOf[AnyRef])
        enrolment.put("completionPercentage", getCompletionPerc(progress, leafNodesCount).asInstanceOf[AnyRef])
      }

      jsonFields.foreach { field =>
        if (enrolment.containsKey(field) && null != enrolment.get(field)) {
          enrolment.put(field, mapper.readTree(enrolment.get(field).asInstanceOf[String]))
        } else {
          enrolment.put(field, new java.util.HashMap[String, AnyRef]())
        }
      }

      // New logic: update contentStatus if recentLanguage is present and contentStatus is null
      val recentLanguage = enrolment.get("recent_language")
      val contentStatus = enrolment.get("contentStatus")
      val languageMapV1 = enrolment.get("langContentStatus").asInstanceOf[java.util.Map[String, AnyRef]]

      if (recentLanguage != null && (contentStatus == null || StringUtils.isBlank(contentStatus.toString)) && languageMapV1 != null) {
        val langKey = recentLanguage.toString.toLowerCase
        val langStatus = languageMapV1.get(langKey)
        if (langStatus != null) {
          enrolment.put("contentStatus", langStatus)
        }
      }
      enrolment
    }
    enrolments
  }

  def getCompletionStatus(completedCount: Int, leafNodesCount: Int): Int = completedCount match {
    case 0 => 0
    case it if 1 until leafNodesCount contains it => 1
    case `leafNodesCount` => 2
    case _ => 2
  }

  def getCompletionPerc(completedCount: Int, leafNodesCount: Int): Int = completedCount match {
    case 0 => 0
    case it if 1 until leafNodesCount contains it => (completedCount * 100) / leafNodesCount
    case `leafNodesCount` => 100
    case _ => 100
  }

  def getExternalEnrollments(userId: String, request: Request): java.util.List[java.util.Map[String, AnyRef]] = {
    var externalEnrolments: java.util.List[java.util.Map[String, AnyRef]] = new java.util.ArrayList()
    val externalEnrolmentsFromDB = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
      request.getRequestContext,
      externalCourseEnrolDbInfo.getKeySpace,
      externalCourseEnrolDbInfo.getTableName,
      JsonKey.USER_ID,
      userId,
      null
    )
    externalEnrolments = externalEnrolmentsFromDB.get(JsonKey.RESPONSE).asInstanceOf[java.util.List[util.Map[String, AnyRef]]]
    if (CollectionUtils.isNotEmpty(externalEnrolments)) {
      externalEnrolments
    } else {
      new util.ArrayList[java.util.Map[String, AnyRef]]()
    }
  }

  def validateEnrolmentV2(batchData: CourseBatch, enrolmentData: UserCourses, isEnrol: Boolean,primaryCategory: String): Unit = {
    if(null == batchData)
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidCourseBatchId, ResponseCode.invalidCourseBatchId.getErrorMessage)

    if(!(EnrolmentType.inviteOnly.getVal.equalsIgnoreCase(batchData.getEnrollmentType) ||
      EnrolmentType.open.getVal.equalsIgnoreCase(batchData.getEnrollmentType)))
      ProjectCommonException.throwClientErrorException(ResponseCode.enrollmentTypeValidation, ResponseCode.enrollmentTypeValidation.getErrorMessage)

    if((2 == batchData.getStatus) || (null != batchData.getEndDate && LocalDateTime.now().isAfter(LocalDate.parse(DATE_FORMAT.format(batchData.getEndDate), DateTimeFormatter.ofPattern("yyyy-MM-dd")).atTime(LocalTime.MAX))))
      ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchAlreadyCompleted, ResponseCode.courseBatchAlreadyCompleted.getErrorMessage)

    if(primaryCategory.equalsIgnoreCase(JsonKey.STANDALONE_ASSESSMENT) && isEnrol && null != batchData.getEnrollmentEndDate &&
      isFutureDate(batchData.getEnrollmentEndDate))
      ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchEnrollmentDateEnded, ResponseCode.courseBatchEnrollmentDateEnded.getErrorMessage)

    if(isEnrol && null != enrolmentData && enrolmentData.isActive) ProjectCommonException.throwClientErrorException(ResponseCode.userAlreadyEnrolledCourse, ResponseCode.userAlreadyEnrolledCourse.getErrorMessage)
    if(!isEnrol && (null == enrolmentData || !enrolmentData.isActive)) ProjectCommonException.throwClientErrorException(ResponseCode.userNotEnrolledCourse, ResponseCode.userNotEnrolledCourse.getErrorMessage)
    if(!isEnrol && ProjectUtil.ProgressStatus.COMPLETED.getValue == enrolmentData.getStatus) ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchAlreadyCompleted, ResponseCode.courseBatchAlreadyCompleted.getErrorMessage)
  }

  def isFutureDate(enrollmentEndDate: Date): Boolean = {
    val inputCal = Calendar.getInstance(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    inputCal.setTime(enrollmentEndDate)
    val currentCal = Calendar.getInstance(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)));
    currentCal.after(inputCal)
  }

  def validateEnrolment(batchData: CourseBatch, enrolmentData: UserCourses, isEnrol: Boolean, isBlendedProgram: Boolean = false): Unit = {
    if(null == batchData) ProjectCommonException.throwClientErrorException(ResponseCode.invalidCourseBatchId, ResponseCode.invalidCourseBatchId.getErrorMessage)

    if(!(EnrolmentType.inviteOnly.getVal.equalsIgnoreCase(batchData.getEnrollmentType) ||
      EnrolmentType.open.getVal.equalsIgnoreCase(batchData.getEnrollmentType)))
      ProjectCommonException.throwClientErrorException(ResponseCode.enrollmentTypeValidation, ResponseCode.enrollmentTypeValidation.getErrorMessage)

    if((2 == batchData.getStatus) || (null != batchData.getEndDate && LocalDateTime.now().isAfter(LocalDate.parse(DATE_FORMAT.format(batchData.getEndDate), DateTimeFormatter.ofPattern("yyyy-MM-dd")).atTime(LocalTime.MAX))))
      ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchAlreadyCompleted, ResponseCode.courseBatchAlreadyCompleted.getErrorMessage)

    if (isBlendedProgram) {
      if (isEnrol && null != batchData.getStartDate && LocalDateTime.now().isAfter(LocalDate.parse(DATE_FORMAT.format(batchData.getStartDate), DateTimeFormatter.ofPattern("yyyy-MM-dd")).atTime(LocalTime.MAX)))
        ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchAlreadyStarted, ResponseCode.courseBatchAlreadyStarted.getErrorMessage)
    }
    if (isEnrol && null != batchData.getEnrollmentEndDate && LocalDateTime.now().isAfter(LocalDate.parse(DATE_FORMAT.format(batchData.getEnrollmentEndDate), DateTimeFormatter.ofPattern("yyyy-MM-dd")).atTime(LocalTime.MAX)))
      ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchEnrollmentDateEnded, ResponseCode.courseBatchEnrollmentDateEnded.getErrorMessage)

    if(isEnrol && null != enrolmentData && enrolmentData.isActive) ProjectCommonException.throwClientErrorException(ResponseCode.userAlreadyEnrolledCourse, ResponseCode.userAlreadyEnrolledCourse.getErrorMessage)
    if(!isEnrol && (null == enrolmentData || !enrolmentData.isActive)) ProjectCommonException.throwClientErrorException(ResponseCode.userNotEnrolledCourse, ResponseCode.userNotEnrolledCourse.getErrorMessage)
    if(!isEnrol && ProjectUtil.ProgressStatus.COMPLETED.getValue == enrolmentData.getStatus) ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchAlreadyCompleted, ResponseCode.courseBatchAlreadyCompleted.getErrorMessage)
  }

  def getCoursesForProgramAndEnrol(request: Request, programId: String, userId: String, batchId: String) = {
    val redisKey = s"$programId:$programId:childrenCourses"
    val childrenNodes: List[String] = cacheUtil.getList(redisKey, redisCollectionIndex)
    val courseBatchMap: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    if (!childrenNodes.isEmpty) {
      for (childNode <- childrenNodes) {
        val contentData = getContentReadAPIData(childNode, List(JsonKey.PRIMARYCATEGORY), request)
        val primaryCategory: String = contentData.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String]
        if (util.Arrays.asList(getConfigValue(JsonKey.PROGRAM_ENROLL_RESTRICTED_CHILDREN_PRIMARY_CATEGORY).split(","): _*).contains(primaryCategory))
          ProjectCommonException.throwClientErrorException(ResponseCode.contentTypeMismatch, childNode)
        else if (util.Arrays.asList(getConfigValue(JsonKey.PROGRAM_ENROLL_ALLOWED_CHILDREN_PRIMARY_CATEGORY).split(","): _*).contains(primaryCategory)) {
          try {
            val batchData: CourseBatch = courseBatchDao.readFirstAvailableBatch(childNode, request.getRequestContext)
            courseBatchMap.put(childNode, batchData)
          } catch {
            case e: ProjectCommonException => ProjectCommonException.throwClientErrorException(ResponseCode.courseDoesNotHaveBatch);
          }
        } else {
          logger.info(request.getRequestContext, "Skipping the enrol for Primary Category" + primaryCategory)
        }
      }
    } else {
      val contentDataForProgram: java.util.List[java.util.Map[String, AnyRef]] = contentHierarchyDao.getContentChildren(request.getRequestContext, programId)
      for (childNode <- contentDataForProgram.asScala) {
        val courseId: String = childNode.get(JsonKey.IDENTIFIER).asInstanceOf[String]
        val primaryCategory: String = childNode.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String]
        if (util.Arrays.asList(getConfigValue(JsonKey.PROGRAM_ENROLL_RESTRICTED_CHILDREN_PRIMARY_CATEGORY).split(","): _*).contains(primaryCategory))
          ProjectCommonException.throwClientErrorException(ResponseCode.contentTypeMismatch, courseId)
        else if (util.Arrays.asList(getConfigValue(JsonKey.PROGRAM_ENROLL_ALLOWED_CHILDREN_PRIMARY_CATEGORY).split(","): _*).contains(primaryCategory)) {
          try {
            val batchData: CourseBatch = courseBatchDao.readFirstAvailableBatch(courseId, request.getRequestContext)
            courseBatchMap.put(courseId, batchData)
          } catch {
            case e: ProjectCommonException => ProjectCommonException.throwClientErrorException(ResponseCode.courseDoesNotHaveBatch);
          }
        } else {
          logger.info(request.getRequestContext, "Skipping the enrol for Primary Category" + primaryCategory)
        }
      }
    }
    for (courseId <- courseBatchMap.keySet()) {
      // Enroll in course with courseId, userId and batchId.
      enrollProgramCourses(request, courseId, courseBatchMap.get(courseId).asInstanceOf[CourseBatch], userId)
    }
  }


  def generatePreProcessorKafkaEvent(request: Request, batchId: String, programId: String, userId: String): Unit = {
    //for generating the kafka event for program generate certificate
    logger.info(request.getRequestContext, "Inside the generatePreProcessorKafkaEvent")
    val ets = System.currentTimeMillis
    val mid = s"""LP.${ets}.${UUID.randomUUID}"""
    val event = s"""{"eid": "BE_JOB_REQUEST","ets": ${ets},"mid": "${mid}","actor": {"id": "Program Certificate Pre Processor Generator","type": "System"},"context": {"pdata": {"ver": "1.0","id": "org.sunbird.platform"}},"object": {"id": "${batchId}_${programId}","type": "ProgramCertificatePreProcessorGeneration"},"edata": {"userId": "${userId}","action": "program-issue-certificate","iteration": 1, "trigger": "auto-issue","batchId": "${batchId}","parentCollections": ["${programId}"],"courseId": "${programId}"}}"""
    val topic = ProjectUtil.getConfigValue("kafka_cert_pre_processor_topic")
    if (StringUtils.isNotBlank(topic)) KafkaClient.send(event, topic)
    else throw new ProjectCommonException("BE_JOB_REQUEST_EXCEPTION", "Invalid topic id.", ResponseCode.CLIENT_ERROR.getResponseCode)
  }

  def getCacheBatchKey(batchId: String) = s"$batchId:active-participants-count"


  def getConsumption(
                      request: Request,
                      userId: String,
                      courseId: String,
                      batchId: String,
                      contentIds: java.util.List[String],
                      language: String,
                      enrolment: java.util.Map[String, AnyRef]
                    ): Unit = {
    val fields = request.getRequest.getOrDefault(JsonKey.FIELDS, new java.util.ArrayList[String]() {
      {
        add(JsonKey.PROGRESS)
      }
    }).asInstanceOf[java.util.List[String]]
    val responseFields: List[String] = getConfigValue(JsonKey.CONSUMPTION_RESPONSE_FIELDS).split(",").map(_.trim).filter(_.nonEmpty).toList
    val contentsConsumed = getContentsConsumption(userId, courseId, contentIds, batchId, language, request.getRequestContext)
    if (CollectionUtils.isNotEmpty(contentsConsumed)) {
      val filteredContents = contentsConsumed.map { m =>
        ProjectUtil.removeUnwantedFields(m, JsonKey.DATE_TIME, JsonKey.USER_ID, JsonKey.ADDED_BY, JsonKey.LAST_UPDATED_TIME, JsonKey.OLD_LAST_ACCESS_TIME, JsonKey.OLD_LAST_UPDATED_TIME, JsonKey.OLD_LAST_COMPLETED_TIME)
        m.put(JsonKey.COLLECTION_ID, m.getOrDefault(JsonKey.COURSE_ID, ""))
        jsonFields.foreach { field =>
          if (m.get(field) != null)
            m.put(field, mapper.readTree(m.get(field).asInstanceOf[String]))
        }
        val resultMap = new java.util.HashMap[String, AnyRef]()
        if (Option(m.get(JsonKey.STATUS)).map(_.asInstanceOf[Integer].intValue()).getOrElse(0) != 2) {
          responseFields.foreach { f =>
            val value: AnyRef = f match {
              case JsonKey.COMPLETION_PERCENTAGE => m.getOrDefault(f, java.lang.Double.valueOf(0.0)).asInstanceOf[AnyRef]
              case JsonKey.STATUS => m.getOrDefault(f, Integer.valueOf(0)).asInstanceOf[AnyRef]
              case _ => m.getOrDefault(f, "").asInstanceOf[AnyRef]
            }
            resultMap.put(f, value)
          }
        } else if (Option(m.get(JsonKey.STATUS)).map(_.asInstanceOf[Integer].intValue()).getOrElse(0) == 2) {
          resultMap.put(JsonKey.CONTENT_ID, m.getOrDefault(JsonKey.CONTENT_ID, "").asInstanceOf[AnyRef])
          resultMap.put(JsonKey.STATUS, m.getOrDefault(JsonKey.STATUS, Integer.valueOf(2)).asInstanceOf[AnyRef])
        }
        val formattedMap = JsonUtil.convertWithDateFormat(resultMap, classOf[util.Map[String, Object]], dateFormatter)
        if (fields.contains(JsonKey.ASSESSMENT_SCORE))
          formattedMap.putAll(scala.collection.JavaConverters.mapAsJavaMap(Map(JsonKey.ASSESSMENT_SCORE -> getScore(userId, courseId, m.get(Constants.CONTENT_ID).asInstanceOf[String], batchId, request.getRequestContext))))
        formattedMap
      }.asJava
      enrolment.put("contentList", filteredContents)
      enrolment.put(JsonKey.LANGUAGE_PROGRESS, getLanguageProgress(userId, courseId, batchId, request.getRequestContext).asJava)
    } else {
      enrolment.put("contentList", new java.util.ArrayList[AnyRef]())
    }
  }

  def getLanguageProgress(
                           userId: String,
                           courseId: String,
                           batchId: String,
                           requestContext: RequestContext
                         ): Map[String, Double] = {

    val filters = Map[String, AnyRef](
      JsonKey.USER_ID_KEY -> userId,
      JsonKey.COURSE_ID_KEY -> courseId,
      JsonKey.BATCH_ID_KEY -> batchId
    ).asJava

    val result = cassandraOperation.getRecords(
      requestContext,
      enrolmentDBInfo.getKeySpace,
      enrolmentDBInfo.getTableName,
      filters,
      null
    )

    val responseList = result.getResult
      .getOrDefault(JsonKey.RESPONSE, new java.util.ArrayList[java.util.Map[String, AnyRef]]())
      .asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]

    if (responseList.isEmpty) return Map.empty

    val langContentStatus = Option(responseList.get(0).get(JsonKey.LANG_CONTENT_STATUS))
      .getOrElse(new java.util.HashMap[String, java.util.Map[String, Integer]]())
      .asInstanceOf[java.util.Map[String, java.util.Map[String, Integer]]]

    val langContentMap: Map[String, Map[String, Int]] = langContentStatus.asScala.map {
      case (lang, contents) => (lang, contents.asScala.map { case (k, v) => (k, v.toInt) }.toMap)
    }.toMap

    val courseMetadata = ContentCacheHandlerV2.getInstance().getContent(courseId)

    val languageMap = Option(courseMetadata.get(JsonKey.LANGUAGE_MAP))
      .map(_.asInstanceOf[java.util.Map[String, java.util.Map[String, AnyRef]]].asScala)
      .getOrElse(Map.empty)

    languageMap.flatMap {
      case (lang, langMeta) =>
        val langCourseId = Option(langMeta.get(JsonKey.ID)).map(_.toString).getOrElse("")
        val completedCount = langContentMap.getOrElse(lang, Map.empty).count(_._2 == 2)

        val courseDetails = ContentCacheHandlerV2.getInstance().getContent(langCourseId)

        val status = Option(langMeta.get(JsonKey.STATUS)).map(_.toString).getOrElse("")
        if (JsonKey.LIVE.equalsIgnoreCase(status)) {
          val leafNodesCount = Option(courseDetails.get(JsonKey.LEAF_NODES))
            .map(_.asInstanceOf[java.util.List[String]].size())
            .getOrElse(0)

          if (leafNodesCount > 0) {
            val percent = (completedCount.toDouble / leafNodesCount) * 100
            Some(lang -> BigDecimal(percent).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
          } else None
        } else None
    }.toMap
  }

  def getContentsConsumption(userId: String, courseId: String, contentIds: java.util.List[String], batchId: String, language: String, requestContext: RequestContext): java.util.List[java.util.Map[String, AnyRef]] = {
    val filters = new java.util.HashMap[String, AnyRef]() {
      {
        put("userid", userId)
        put("courseid", courseId)
        put("batchid", batchId)
        put("language", language)
        if (CollectionUtils.isNotEmpty(contentIds))
          put("contentid", contentIds)
      }
    }
    val response = cassandraOperation.getRecords(requestContext, consumptionDBInfo.getKeySpace, consumptionDBInfo.getTableName, filters, null)
    response.getResult.getOrDefault(JsonKey.RESPONSE, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
  }

  def getScore(userId: String, courseId: String, contentId: String, batchId: String, requestContext: RequestContext): util.List[util.Map[String, AnyRef]] = {
    val filters = new java.util.HashMap[String, AnyRef]() {
      {
        put("user_id", userId)
        put("course_id", courseId)
        put("batch_id", batchId)
        put("content_id", contentId)
      }
    }
    val fieldsToGet = new java.util.ArrayList[String]() {
      {
        add("attempt_id")
        add("last_attempted_on")
        add("total_max_score")
        add("total_score")
      }
    }
    val limit = if (StringUtils.isNotBlank(ProjectUtil.getConfigValue("assessment.attempts.limit")))
      (ProjectUtil.getConfigValue("assessment.attempts.limit")).asInstanceOf[Integer] else 25.asInstanceOf[Integer]
    val response = cassandraOperation.getRecordsWithLimit(requestContext, assessmentAggregatorDBInfo.getKeySpace, assessmentAggregatorDBInfo.getTableName, filters, fieldsToGet, limit)
    response.getResult.getOrDefault(JsonKey.RESPONSE, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
  }

  def enrollProgramCourses(request: Request,courseId: String,batchData:CourseBatch, userId: String): Boolean = {
    try {
      val recentLanguage: String = request.get(JsonKey.RECENT_LANGUAGE).asInstanceOf[String]
      val batchId: String = batchData.getBatchId.asInstanceOf[String]
      var enrolmentData: util.List[UserCourses] = userCoursesDao.extendedReadV2(request.getRequestContext, userId, courseId)
      if (CollectionUtils.isEmpty(enrolmentData)) {
        enrolmentData = new util.ArrayList[UserCourses]();
      }
      val batchUserData: BatchUser = batchUserDao.read(request.getRequestContext, batchId, userId)
      validateEnrolmentV3(batchData, enrolmentData, true)

      val dataBatch: util.Map[String, AnyRef] = createBatchUserMapping(batchId, userId, batchUserData)
      val existingEnrolmentForTheBatch: UserCourses = enrolmentData.find(_.getBatchId == batchId).orNull
      val data: java.util.Map[String, AnyRef] = createUserEnrolmentMap(userId, courseId, batchId, existingEnrolmentForTheBatch, request.getContext.getOrDefault(JsonKey.REQUEST_ID, "").asInstanceOf[String], request.getRequestContext, recentLanguage)
      upsertEnrollment(userId, courseId, batchId, data, dataBatch, (null == existingEnrolmentForTheBatch), request.getRequestContext)
      logger.info(request.getRequestContext, "CourseEnrolmentActor :: enroll :: Deleting redis for key " + getCacheKey(userId))
      cacheUtil.delete(getCacheKey(userId))
      generateTelemetryAudit(userId, courseId, batchId, data, "enrol", JsonKey.CREATE, request.getContext)
      notifyUser(userId, batchData, JsonKey.ADD , recentLanguage)
    } catch {
      case e: ProjectCommonException =>
        if (ResponseCode.userAlreadyEnrolledCourse.getErrorMessage.equals(e.getMessage))
          return true
        if (ResponseCode.userAlreadyEnrolledCourseWithDifferentBatch.getErrorMessage.equals(e.getMessage))
          return true
        if (ResponseCode.userAlreadyCompletedCourse.getErrorMessage.equals(e.getMessage))
          return true
        if (ResponseCode.courseBatchEnrollmentDateEnded.getErrorMessage.equals(e.getMessage))
          ProjectCommonException.throwClientErrorException(ResponseCode.courseBatchEnrollmentDateEnded, ResponseCode.courseBatchEnrollmentDateEnded.getErrorMessage)
        if (ResponseCode.userNotEnrolledCourse.getErrorMessage.equals(e.getMessage))
          ProjectCommonException.throwClientErrorException(ResponseCode.userNotEnrolledCourse, ResponseCode.userNotEnrolledCourse.getErrorMessage)
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in upsertEnrollment list : user ::" + e.getMessage, e)
        ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, request.get(JsonKey.COURSE_ID).asInstanceOf[String]);
    }
    false;
  }

  def getBatchFrmLocalCacheV2(batchId: String, courseId: String): java.util.Map[String, AnyRef] = {
    try {
      val batch = BatchCacheHandlerV2.getInstance().getContent(batchId, courseId)
      if (batch != null && !batch.isEmpty) {
        batch.asInstanceOf[java.util.Map[String, AnyRef]]
      } else {
        null
      }
    } catch {
      case ex: Exception =>
        logger.error(null, s"getBatchFrmLocalCacheV2: Exception while retrieving batch for batchId: $batchId and courseId: $courseId", ex)
        null
    }
  }

  private def parseContentAttributesFromUrl(request: Request): java.util.List[String] = {
    val urlObj = request.getContext.get(JsonKey.URL)
    val urlQueryString = if (urlObj != null) urlObj.asInstanceOf[String] else ""
    val contentAttributes = new util.ArrayList[String]()

    if (StringUtils.isNotBlank(urlQueryString) && urlQueryString.contains("?")) {
      val queryString = urlQueryString.split("\\?", 2)(1)
      if (StringUtils.isNotBlank(queryString)) {
        val params = queryString.split("&")

        for (p <- params if StringUtils.isNotBlank(p)) {
          val parts = p.split("=", 2)
          if (parts.length == 2 && parts(0) == JsonKey.CONTENT_ATTRIBUTES) {
            val valueParts = parts(1).split(",")
            valueParts.foreach { v =>
              val trimmed = Option(v).map(_.trim).getOrElse("")
              if (trimmed.nonEmpty) contentAttributes.add(trimmed)
            }
          }
        }
      }
    }
    contentAttributes
  }


  def enrollMilestoneCourse(request: Request, courseId: String, userId: String): Unit = {
    val courseBatchMap: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    val contentData = getContentReadAPIData(courseId, List(JsonKey.PRIMARYCATEGORY), request)
    val primaryCategory: String = contentData.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String]
    if (util.Arrays.asList(getConfigValue(JsonKey.PROGRAM_ENROLL_RESTRICTED_CHILDREN_PRIMARY_CATEGORY).split(","): _*).contains(primaryCategory))
      ProjectCommonException.throwClientErrorException(ResponseCode.contentTypeMismatch, courseId)
    else if (util.Arrays.asList(getConfigValue(JsonKey.PROGRAM_ENROLL_ALLOWED_CHILDREN_PRIMARY_CATEGORY).split(","): _*).contains(primaryCategory)) {
      try {
        val batchData: CourseBatch = courseBatchDao.readFirstAvailableBatch(courseId, request.getRequestContext)
        courseBatchMap.put(courseId, batchData)
      } catch {
        case e: ProjectCommonException => ProjectCommonException.throwClientErrorException(ResponseCode.courseDoesNotHaveBatch);
      }
    } else {
      logger.info(request.getRequestContext, "Skipping the enrol for Primary Category" + primaryCategory)
    }
    enrollProgramCourses(request, courseId, courseBatchMap.get(courseId).asInstanceOf[CourseBatch], userId)
  }

  def validateLearningPathwayContent(contentData: util.Map[String, AnyRef]): Unit = {
    val status = contentData.get(JsonKey.STATUS).asInstanceOf[String]
    if (!JsonKey.LIVE.equalsIgnoreCase(status)) {
      ProjectCommonException.throwClientErrorException(
        ResponseCode.invalidParameterValue,
        s"Content status must be Live. Current status: $status"
      )
    }

    val courseCategory = contentData.get(JsonKey.COURSECATEGORY).asInstanceOf[String]
    if (!JsonKey.LEARNING_PATHWAY.equalsIgnoreCase(courseCategory)) {
      ProjectCommonException.throwClientErrorException(
        ResponseCode.invalidParameterValue,
        s"Course category must be 'Learning Pathway'. Current category: $courseCategory"
      )
    }

    val milestonesObj = contentData.get(JsonKey.MILESTONES_V1)
    if (null == milestonesObj || !milestonesObj.isInstanceOf[java.util.List[_]] || milestonesObj.asInstanceOf[java.util.List[_]].isEmpty) {
      ProjectCommonException.throwClientErrorException(
        ResponseCode.invalidParameterValue,
        "No milestones found in Learning Pathway"
      )
    }

    val batches = contentData.get(JsonKey.BATCHES).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    if (CollectionUtils.isEmpty(batches)) {
      ProjectCommonException.throwClientErrorException(
        ResponseCode.learningPathwayBatchNotFound,
        ResponseCode.learningPathwayBatchNotFound.getErrorMessage
      )
    }
  }

  def addLearningPathwayCourseIds(request: Request, contentData: util.Map[String, AnyRef], courseIdList:  java.util.List[String]): Unit = {
    val milestones = contentData.get(JsonKey.MILESTONES_V1).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    if (CollectionUtils.isNotEmpty(milestones)) {
      for (milestone <- milestones.asScala) {
        val courses = milestone.get(JsonKey.COURSES).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        if (CollectionUtils.isNotEmpty(courses)) {
          for (course <- courses.asScala) {
            courseIdList.add(course.get(JsonKey.IDENTIFIER).asInstanceOf[String])
          }
        }
      }
    }
  }

  /**
   * Computes badge statistics following a 7-step flow:
   * STEP 1: Enrolments already fetched from Cassandra (user_enrolments_v2)
   * STEP 2: Extract course IDs from enrolments
   * STEP 3: Call composite search API to filter badge courses
   * STEP 4: Build courseId → badgeDetails map
   * STEP 5: Split enrolments by status (completed vs in-progress)
   * STEP 6A: Process completed badge courses
   * STEP 6B: Process in-progress badge courses with expiry filter
   * STEP 7: Build final response with summary and details
   *
   * OPTIMIZATION: Results cached in Redis with 1-hour TTL
   *
   * @param statusFilter Optional filter: "Completed" or "In-Progress"
   */
  def getBadgeStats(
    request: Request,
    userId: String,
    enrolments: java.util.List[java.util.Map[String, AnyRef]],
    statusFilter: String = null,
    isExternal: Boolean = false
  ): java.util.Map[String, AnyRef] = {



    logger.info(request.getRequestContext, s"Badge stats cache MISS for userId: $userId, statusFilter: $statusFilter - computing fresh")

    val now = System.currentTimeMillis()

    // STEP 2: Extract course IDs from enrolments
    val courseIds: java.util.List[String] = enrolments.asScala
      .map(e => e.get(JsonKey.COURSE_ID).asInstanceOf[String])
      .distinct
      .asJava

    logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getBadgeStats :: Processing ${enrolments.size()} enrollments with ${courseIds.size()} unique courses")

    if (courseIds.isEmpty) {
      logger.info(request.getRequestContext, "ExtendedBadgeEnrollmentActor :: getBadgeStats :: No course IDs found - returning empty response")
      return createEmptyBadgeStatsResponse()
    }

    // STEP 3: Search API — Filter badge courses (with built-in content data)
    logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getBadgeStats :: About to call fetchBadgeCoursesFromSearch with ${courseIds.size()} course IDs, isExternal=$isExternal")
    val badgeCourseMap: Map[String, (java.util.List[java.util.Map[String, AnyRef]], String, Int)] =
      fetchBadgeCoursesFromSearch(courseIds, request, isExternal)

    logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getBadgeStats :: fetchBadgeCoursesFromSearch returned ${badgeCourseMap.size} badge courses from search out of ${courseIds.size()} total courses")

    // STEP 4: Filter enrolments to only badge courses
    val badgeEnrolments = enrolments.asScala.filter { e =>
      val courseId = e.get(JsonKey.COURSE_ID).asInstanceOf[String]
      badgeCourseMap.contains(courseId)
    }.toList

    logger.info(request.getRequestContext, s"ExtendedBadgeEnrollmentActor :: getBadgeStats :: Filtered to ${badgeEnrolments.size} badge enrollments")

    if (badgeEnrolments.isEmpty) {
      logger.info(request.getRequestContext, "ExtendedBadgeEnrollmentActor :: getBadgeStats :: No badge enrollments found - returning empty response")
      val emptyResponse = createEmptyBadgeStatsResponse()
      // Cache disabled for testing
      // cacheStats(cacheKey, emptyResponse, request)
      return emptyResponse
    }

    // STEP 5: Split by status
    val completedEnrolments = badgeEnrolments.filter { e =>
      Option(e.get(JsonKey.STATUS)).map(_.asInstanceOf[Integer].intValue()).getOrElse(0) == 2
    }

    val inProgressEnrolments = badgeEnrolments.filter { e =>
      val status = Option(e.get(JsonKey.STATUS)).map(_.asInstanceOf[Integer].intValue()).getOrElse(0)
      status != 2
    }

    // STEP 6A: Process completed badge courses
    val completedBadgesDetails = completedEnrolments.flatMap { e =>
      val courseId = e.get(JsonKey.COURSE_ID).asInstanceOf[String]
      badgeCourseMap.get(courseId).flatMap { case (badges, courseName, leafNodesCount) =>
        val issuedBadges = Option(e.get(JsonKey.ISSUED_BADGES))
          .collect { case l: java.util.List[_] if !l.isEmpty => l }
          .getOrElse(new java.util.ArrayList())

        if (!issuedBadges.isEmpty) {
          Some(createCompletedBadgeDetail(e, badges, courseId, courseName))
        } else {
          None
        }
      }
    }

    // STEP 6B: Process in-progress badge courses with expiry filter
    val inProgressBadgesDetails = inProgressEnrolments.flatMap { e =>
      val courseId = e.get(JsonKey.COURSE_ID).asInstanceOf[String]
      badgeCourseMap.get(courseId).flatMap { case (badges, courseName, leafNodesCount) =>

        // Check if at least one badge is within expiry
        val hasValidBadge = badges.asScala.exists { badge =>
          val dateEnabled = Option(badge.get(JsonKey.BADGE_EARNING_DATE_ENABLED))
            .map(_.asInstanceOf[Boolean]).getOrElse(false)

          if (!dateEnabled) {
            true // Always valid
          } else {
            Option(badge.get(JsonKey.BADGE_EARNING_DATE_TIME))
              .map(_.asInstanceOf[Number].longValue() > now)
              .getOrElse(false)
          }
        }

        if (hasValidBadge) {
          Some(createInProgressBadgeDetail(e, badges, courseId, courseName, leafNodesCount, now))
        } else {
          None
        }
      }
    }

    val courseCompleted = enrolments.asScala.count { e =>
      Option(e.get(JsonKey.STATUS)).map(_.asInstanceOf[Integer].intValue()).getOrElse(0) == 2
    }

    // totalBadgesEarned = Completed courses that have issued_badges (subset of courseCompleted)
    val totalBadgesEarned = completedBadgesDetails.size

    // Sort in-progress badges by completionPercentage in descending order
    val sortedInProgressBadges = inProgressBadgesDetails.sortBy { badge =>
      -Option(badge.get(JsonKey.COMPLETION_PERCENTAGE))
        .map(_.asInstanceOf[Number].intValue())
        .getOrElse(0)
    }

    val totalBadgesAttempted = totalBadgesEarned + sortedInProgressBadges.size
    val completionRate = if (totalBadgesAttempted > 0) {
      (totalBadgesEarned * 100) / totalBadgesAttempted
    } else {
      0
    }

    // STEP 7: Build final response
    val summary = new java.util.HashMap[String, AnyRef]()
    summary.put(JsonKey.TOTAL_BADGES_EARNED, totalBadgesEarned.asInstanceOf[AnyRef])
    summary.put(JsonKey.COURSE_COMPLETED, courseCompleted.asInstanceOf[AnyRef])
    summary.put(JsonKey.COMPLETION_RATE, completionRate.asInstanceOf[AnyRef])

    val earnedBadgesDetails = new java.util.HashMap[String, AnyRef]()
    earnedBadgesDetails.put(JsonKey.COUNT, completedBadgesDetails.size.asInstanceOf[AnyRef])
    earnedBadgesDetails.put(JsonKey.BADGES, completedBadgesDetails.asJava)

    val inProgressBadgesDetailsMap = new java.util.HashMap[String, AnyRef]()
    inProgressBadgesDetailsMap.put(JsonKey.COUNT, sortedInProgressBadges.size.asInstanceOf[AnyRef])
    inProgressBadgesDetailsMap.put(JsonKey.BADGES, sortedInProgressBadges.asJava)

    val result = new java.util.HashMap[String, AnyRef]()
    result.put(JsonKey.SUMMARY, summary)
    result.put(JsonKey.EARNED_BADGES_DETAILS, earnedBadgesDetails)
    result.put(JsonKey.IN_PROGRESS_BADGES_DETAILS, inProgressBadgesDetailsMap)

    logger.info(request.getRequestContext, s"getBadgeStats :: Returning result with ${inProgressBadgesDetails.size} in-progress badges")
    logger.info(request.getRequestContext, s"getBadgeStats :: Result JSON: ${mapper.writeValueAsString(result)}")

    // Cache disabled for testing
    // cacheStats(cacheKey, result, request)

    result
  }

  private def cacheStats(
    cacheKey: String,
    stats: java.util.Map[String, AnyRef],
    request: Request
  ): Unit = {
    try {
      cacheUtil.set(cacheKey, mapper.writeValueAsString(stats), 3600) // 1-hour TTL
      logger.info(request.getRequestContext, s"Badge stats cached for key: $cacheKey")
    } catch {
      case e: Exception =>
        logger.info(request.getRequestContext, s"Failed to cache badge stats: ${e.getMessage}")
    }
  }

  private def createEmptyBadgeStatsResponse(): java.util.Map[String, AnyRef] = {
    val summary = new java.util.HashMap[String, AnyRef]()
    summary.put(JsonKey.TOTAL_BADGES_EARNED, 0.asInstanceOf[AnyRef])
    summary.put(JsonKey.COURSE_COMPLETED, 0.asInstanceOf[AnyRef])

    val earnedBadgesDetails = new java.util.HashMap[String, AnyRef]()
    earnedBadgesDetails.put(JsonKey.COUNT, 0.asInstanceOf[AnyRef])
    earnedBadgesDetails.put(JsonKey.BADGES, new java.util.ArrayList())

    val inProgressBadgesDetails = new java.util.HashMap[String, AnyRef]()
    inProgressBadgesDetails.put(JsonKey.COUNT, 0.asInstanceOf[AnyRef])
    inProgressBadgesDetails.put(JsonKey.BADGES, new java.util.ArrayList())

    val result = new java.util.HashMap[String, AnyRef]()
    result.put(JsonKey.SUMMARY, summary)
    result.put(JsonKey.EARNED_BADGES_DETAILS, earnedBadgesDetails)
    result.put(JsonKey.IN_PROGRESS_BADGES_DETAILS, inProgressBadgesDetails)
    result
  }

  private def fetchBadgeCoursesFromSearch(
    courseIds: java.util.List[String],
    request: Request,
    isExternal: Boolean = false
  ): Map[String, (java.util.List[java.util.Map[String, AnyRef]], String, Int)] = {
    logger.info(request.getRequestContext, s"fetchBadgeCoursesFromSearch :: ENTRY :: courseIds.size=${courseIds.size()}, isExternal=$isExternal")
    try {
      // Get max identifier size from config (default: 100)
      val searchIdentifierMaxSize = try {
        Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.SEARCH_IDENTIFIER_MAX_SIZE))
      } catch {
        case _: Exception => 100 // Default to 100 if config not found
      }

      logger.info(request.getRequestContext,
        s"fetchBadgeCoursesFromSearch :: Fetching badge courses for ${courseIds.size()} course IDs (batch size: $searchIdentifierMaxSize)")

      // If courseIds exceed max size, batch the requests
      if (courseIds.size() > searchIdentifierMaxSize) {
        logger.info(request.getRequestContext,
          s"fetchBadgeCoursesFromSearch :: Course IDs (${courseIds.size()}) exceed max size ($searchIdentifierMaxSize) - batching requests")

        val batches = courseIds.asScala.grouped(searchIdentifierMaxSize).toList
        val result = batches.flatMap { batch =>
          fetchBadgeCoursesFromSearchBatch(batch.asJava, request, isExternal)
        }.toMap
        
        logger.info(request.getRequestContext, s"fetchBadgeCoursesFromSearch :: Batching complete, returning ${result.size} results")
        result

      } else {
        logger.info(request.getRequestContext, s"fetchBadgeCoursesFromSearch :: Calling fetchBadgeCoursesFromSearchBatch directly")
        val result = fetchBadgeCoursesFromSearchBatch(courseIds, request, isExternal)
        logger.info(request.getRequestContext, s"fetchBadgeCoursesFromSearch :: fetchBadgeCoursesFromSearchBatch returned ${result.size} results")
        result
      }

    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, s"fetchBadgeCoursesFromSearch :: EXCEPTION :: ${e.getMessage}", e)
        Map.empty
    }
  }

  private def fetchBadgeCoursesFromSearchBatch(
    courseIds: java.util.List[String],
    request: Request,
    isExternal: Boolean = false
  ): Map[String, (java.util.List[java.util.Map[String, AnyRef]], String, Int)] = {
    try {
      logger.info(request.getRequestContext,
        s"fetchBadgeCoursesFromSearchBatch :: Processing ${courseIds.size()} course IDs")

      // Build search request
      val searchRequest = new java.util.HashMap[String, AnyRef]()
      val filters = new java.util.HashMap[String, AnyRef]()
      filters.put(JsonKey.IDENTIFIER, courseIds)

      val badgeFilters = new java.util.ArrayList[Boolean]()
      badgeFilters.add(true)
      badgeFilters.add(false)
      filters.put(s"${JsonKey.BADGE_DETAILS_V1}.${JsonKey.BADGE_EARNING_DATE_ENABLED}", badgeFilters)

      searchRequest.put(JsonKey.FILTERS, filters)

      val fields = new java.util.ArrayList[String]()
      fields.add(JsonKey.IDENTIFIER)
      fields.add(JsonKey.BADGE_DETAILS_V1)
      fields.add(JsonKey.NAME)
      fields.add(JsonKey.LEAF_NODE_COUNT)
      searchRequest.put(JsonKey.FIELDS, fields)

      val requestBody = new java.util.HashMap[String, AnyRef]()
      requestBody.put(JsonKey.REQUEST, searchRequest)

      val headers = new java.util.HashMap[String, String]()
      headers.put(JsonKey.CONTENT_TYPE, "application/json")

      logger.info(request.getRequestContext,
        s"fetchBadgeCoursesFromSearchBatch :: Calling search API with payload: ${mapper.writeValueAsString(requestBody)}")

      // Use different search API based on isExternal
      val searchResult = if (isExternal) {
        // For external courses, use CIOS API
        logger.info(request.getRequestContext, "fetchBadgeCoursesFromSearchBatch :: Using CIOS API for external courses")
        searchExternalContent(requestBody, headers, request)
      } else {
        // For internal courses, use composite search API
        logger.info(request.getRequestContext, "fetchBadgeCoursesFromSearchBatch :: Using composite search API for internal courses")
        ContentUtil.searchContent(mapper.writeValueAsString(requestBody), headers)
      }

      logger.info(request.getRequestContext,
        s"fetchBadgeCoursesFromSearchBatch :: Search API response: ${mapper.writeValueAsString(searchResult)}")

      val contents = Option(searchResult.get(JsonKey.CONTENTS))
        .collect { case l: java.util.List[java.util.Map[String, AnyRef]] => l }
        .getOrElse(new java.util.ArrayList())

      logger.info(request.getRequestContext,
        s"fetchBadgeCoursesFromSearchBatch :: Search returned ${contents.size()} badge courses out of ${courseIds.size()} requested")

      // Build map of courseId -> (badgeDetails, courseName, leafNodesCount)
      // This eliminates N+1 getCourseContent() calls
      val resultMap = contents.asScala.flatMap { content =>
        val identifier = content.get(JsonKey.IDENTIFIER).asInstanceOf[String]
        val courseName = Option(content.get(JsonKey.NAME))
          .map(_.asInstanceOf[String])
          .getOrElse("")
        val leafNodesCount = Option(content.get(JsonKey.LEAF_NODE_COUNT))
          .map(_.asInstanceOf[Number].intValue())
          .getOrElse(0)
        val badgeDetails = Option(content.get(JsonKey.BADGE_DETAILS_V1))
          .collect { case l: java.util.List[java.util.Map[String, AnyRef]] => l }

        if (badgeDetails.isDefined) {
          logger.info(request.getRequestContext,
            s"fetchBadgeCoursesFromSearchBatch :: Found badge details for course: $identifier (${badgeDetails.get.size()} badges, leafNodesCount: $leafNodesCount)")
        } else {
          logger.info(request.getRequestContext,
            s"fetchBadgeCoursesFromSearchBatch :: No badge details for course: $identifier")
        }

        badgeDetails.map(bd => identifier -> (bd, courseName, leafNodesCount))
      }.toMap

      logger.info(request.getRequestContext,
        s"fetchBadgeCoursesFromSearchBatch :: Returning ${resultMap.size} courses with badge details")

      resultMap

    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, s"fetchBadgeCoursesFromSearchBatch :: Failed to fetch badge courses batch from search: ${e.getMessage}", e)
        Map.empty
    }
  }

  private def createCompletedBadgeDetail(
    enrolment: java.util.Map[String, AnyRef],
    badges: java.util.List[java.util.Map[String, AnyRef]],
    courseId: String,
    courseName: String
  ): java.util.Map[String, AnyRef] = {
    val detail = new java.util.HashMap[String, AnyRef]()

    detail.put(JsonKey.COURSE_ID, courseId)
    detail.put(JsonKey.COURSE_NAME, courseName)

    // Include full badgeDetails_v1 array
    detail.put(JsonKey.BADGE_DETAILS_V1, badges)


    detail
  }

  private def createInProgressBadgeDetail(
    enrolment: java.util.Map[String, AnyRef],
    badges: java.util.List[java.util.Map[String, AnyRef]],
    courseId: String,
    courseName: String,
    leafNodesCount: Int,
    now: Long
  ): java.util.Map[String, AnyRef] = {
    val detail = new java.util.HashMap[String, AnyRef]()

    detail.put(JsonKey.COURSE_ID, courseId)
    detail.put(JsonKey.COURSE_NAME, courseName)
    detail.put(JsonKey.BADGE_DETAILS_V1, badges)

    // Get progress from enrolment
    val progress = Option(enrolment.get(JsonKey.PROGRESS))
      .map(_.asInstanceOf[Number].intValue())
      .getOrElse(0)
    detail.put(JsonKey.PROGRESS, progress.asInstanceOf[AnyRef])

    // Calculate completion percentage using the same logic as CourseEnrollmentActor
    // For external courses (leafNodesCount = 0), default to 0
    val completionPercentage = if (leafNodesCount > 0) {
      getCompletionPerc(progress, leafNodesCount)
    } else {
      0 // Default for external courses where leafNodesCount is not available
    }
    detail.put(JsonKey.COMPLETION_PERCENTAGE, completionPercentage.asInstanceOf[AnyRef])
    detail
  }

  def getUserBadgeCount(requestContext: RequestContext, userId: String): Int = {
    val redisKey = JsonKey.USER_BADGE_COUNT_REDIS_KEY + userId
    try {
      val cachedValue = cacheUtil.get(redisKey)
      if (cachedValue != null && cachedValue.nonEmpty) {
        return cachedValue.toInt
      }
      val badgeResponse = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
        requestContext,
        badgeDbInfo.getKeySpace,
        badgeDbInfo.getTableName,
        JsonKey.USER_ID,
        userId,
        util.Arrays.asList(JsonKey.COURSE_ID)
      )
      val badgeRecords: java.util.List[util.Map[String, AnyRef]] = badgeResponse.get(JsonKey.RESPONSE).asInstanceOf[java.util.List[util.Map[String, AnyRef]]]
      val totalBadgeCount: Int = if (CollectionUtils.isEmpty(badgeRecords)) 0 else badgeRecords.size()
      cacheUtil.set(redisKey, totalBadgeCount.toString)
      totalBadgeCount
    } catch {
      case e: Exception =>
        logger.warn(null, s"Failed to fetch badge count for userId $userId: ${e.getMessage}", e)
        0
    }
  }

  /**
   * Search external content using CIOS API
   * CIOS API uses different structure: filterCriteriaMap, requestedFields, contentId
   */
  private def searchExternalContent(
    requestBody: java.util.Map[String, AnyRef],
    headers: java.util.Map[String, String],
    request: Request
  ): java.util.Map[String, AnyRef] = {
    try {
      // Construct full CIOS API URL: base URL + endpoint path
      val ciosBaseUrl = ProjectUtil.getConfigValue(JsonKey.CB_PORES_SERVICE_BASE_URL)
      val ciosSearchPath = ProjectUtil.getConfigValue(JsonKey.CB_PORES_CIOS_EXTERNAL_CONTENT_SEARCH_BASE_URL)
      val ciosSearchUrl = ciosBaseUrl + ciosSearchPath
      logger.info(request.getRequestContext, s"searchExternalContent :: Calling CIOS API at: $ciosSearchUrl")

      // Transform composite search request to CIOS format
      val searchRequest = requestBody.get(JsonKey.REQUEST).asInstanceOf[java.util.Map[String, AnyRef]]
      val filters = searchRequest.get(JsonKey.FILTERS).asInstanceOf[java.util.Map[String, AnyRef]]
      val fields = searchRequest.get(JsonKey.FIELDS).asInstanceOf[java.util.List[String]]

      // Build CIOS request format
      val ciosRequestBody = new java.util.HashMap[String, AnyRef]()

      // filterCriteriaMap - convert from composite search filters
      val filterCriteriaMap = new java.util.HashMap[String, AnyRef]()

      // Add contentId filter (mapped from identifier)
      if (filters.containsKey(JsonKey.IDENTIFIER)) {
        val courseIds = filters.get(JsonKey.IDENTIFIER).asInstanceOf[java.util.List[String]]
        filterCriteriaMap.put("contentId", courseIds)
      }

      // Add badgeDetails_v1.badgeEarningDateEnabled filter
      val badgeFilterKey = s"${JsonKey.BADGE_DETAILS_V1}.${JsonKey.BADGE_EARNING_DATE_ENABLED}"
      if (filters.containsKey(badgeFilterKey)) {
        filterCriteriaMap.put("badgeDetails_v1.badgeEarningDateEnabled", filters.get(badgeFilterKey))
      }

      // Add contentPartner.isActive filter for CIOS
      filterCriteriaMap.put("contentPartner.isActive", java.lang.Boolean.TRUE)

      ciosRequestBody.put("filterCriteriaMap", filterCriteriaMap)

      // requestedFields - map field names
      val requestedFields = new java.util.ArrayList[String]()
      if (fields != null) {
        fields.asScala.foreach {
          case JsonKey.IDENTIFIER => requestedFields.add("contentId")  // Map identifier to contentId
          case JsonKey.NAME => requestedFields.add("name")
          case JsonKey.BADGE_DETAILS_V1 => requestedFields.add("badgeDetails_v1")
          case JsonKey.LEAF_NODE_COUNT => requestedFields.add("leafNodesCount")  // May not exist in CIOS
          case other => requestedFields.add(other)
        }
      }
      ciosRequestBody.put("requestedFields", requestedFields)

      logger.info(request.getRequestContext, s"searchExternalContent :: CIOS request: ${mapper.writeValueAsString(ciosRequestBody)}")

      // Update headers for CIOS API - ensure correct Content-Type without charset
      val ciosHeaders = new java.util.HashMap[String, String]()
      ciosHeaders.put("Content-Type", "application/json")  // No charset
      ciosHeaders.put("accept", "*/*")

      // Copy authorization header if present
      if (headers.containsKey("Authorization")) {
        ciosHeaders.put("Authorization", headers.get("Authorization"))
      }

      logger.info(request.getRequestContext, s"searchExternalContent :: Using headers: $ciosHeaders")

      val response = HttpUtil.sendPostRequest(ciosSearchUrl, mapper.writeValueAsString(ciosRequestBody), ciosHeaders)

      if (response != null && response.nonEmpty) {
        val ciosResponse = mapper.readValue(response, classOf[java.util.Map[String, AnyRef]])
        logger.info(request.getRequestContext, s"searchExternalContent :: CIOS API returned response")

        // Transform CIOS response to composite search format for compatibility
        val transformedResponse = transformCiosResponseToCompositeFormat(ciosResponse, request)
        transformedResponse
      } else {
        logger.info(request.getRequestContext, "searchExternalContent :: Empty response from CIOS API")
        createEmptySearchResponse()
      }
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, s"searchExternalContent :: Error calling CIOS API: ${e.getMessage}", e)
        createEmptySearchResponse()
    }
  }

  /**
   * Transform CIOS API response to Composite Search API format for compatibility
   */
  private def transformCiosResponseToCompositeFormat(
    ciosResponse: java.util.Map[String, AnyRef],
    request: Request
  ): java.util.Map[String, AnyRef] = {
    try {
      val result = new java.util.HashMap[String, AnyRef]()

      // CIOS response typically has: { "data": [...], "count": X }
      // We need: { "contents": [...], "count": X }
      val data = Option(ciosResponse.get("data"))
        .orElse(Option(ciosResponse.get("content")))
        .orElse(Option(ciosResponse.get("contents")))
        .collect { case l: java.util.List[java.util.Map[String, AnyRef]] => l }
        .getOrElse(new java.util.ArrayList[java.util.Map[String, AnyRef]]())

      // Transform each content item: contentId → identifier
      val transformedContents = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
      data.asScala.foreach { item =>
        val transformed = new java.util.HashMap[String, AnyRef]()

        // Map contentId to identifier
        if (item.containsKey("contentId")) {
          transformed.put(JsonKey.IDENTIFIER, item.get("contentId"))
        }

        // Copy other fields as-is
        item.asScala.foreach {
          case ("contentId", _) => // Already mapped to identifier
          case (key, value) => transformed.put(key, value)
        }

        // Set leafNodesCount to 0 for external courses (not provided by CIOS)
        if (!transformed.containsKey(JsonKey.LEAF_NODE_COUNT)) {
          transformed.put(JsonKey.LEAF_NODE_COUNT, Integer.valueOf(0))
        }

        transformedContents.add(transformed)
      }

      result.put(JsonKey.CONTENTS, transformedContents)
      result.put(JsonKey.COUNT, transformedContents.size().asInstanceOf[AnyRef])

      logger.info(request.getRequestContext, s"searchExternalContent :: Transformed ${transformedContents.size()} CIOS contents to composite format")
      result
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, s"transformCiosResponseToCompositeFormat :: Error: ${e.getMessage}", e)
        createEmptySearchResponse()
    }
  }

  /**
   * Create empty search response
   */
  private def createEmptySearchResponse(): java.util.Map[String, AnyRef] = {
    val response = new java.util.HashMap[String, AnyRef]()
    response.put(JsonKey.CONTENTS, new java.util.ArrayList[java.util.Map[String, AnyRef]]())
    response.put(JsonKey.COUNT, Integer.valueOf(0))
    response
  }

  /**
   * Merge two badge detail maps (earned or in-progress)
   */
  private def mergeBadgeLists(
    internal: java.util.Map[String, AnyRef],
    external: java.util.Map[String, AnyRef]
  ): java.util.Map[String, AnyRef] = {
    val merged = new java.util.HashMap[String, AnyRef]()

    val internalBadges = internal.get(JsonKey.BADGES).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    val externalBadges = external.get(JsonKey.BADGES).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]

    val allBadges = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
    if (internalBadges != null) allBadges.addAll(internalBadges)
    if (externalBadges != null) allBadges.addAll(externalBadges)

    // Sort merged list by completionPercentage DESC (for in-progress badges)
    if (allBadges.size() > 0 && allBadges.get(0).containsKey(JsonKey.COMPLETION_PERCENTAGE)) {
      allBadges.sort((a, b) => {
        val percA = Option(a.get(JsonKey.COMPLETION_PERCENTAGE)).map(_.asInstanceOf[Number].intValue()).getOrElse(0)
        val percB = Option(b.get(JsonKey.COMPLETION_PERCENTAGE)).map(_.asInstanceOf[Number].intValue()).getOrElse(0)
        percB.compareTo(percA) // Descending order
      })
    }

    merged.put(JsonKey.COUNT, allBadges.size().asInstanceOf[AnyRef])
    merged.put(JsonKey.BADGES, allBadges)
    merged
  }
}
