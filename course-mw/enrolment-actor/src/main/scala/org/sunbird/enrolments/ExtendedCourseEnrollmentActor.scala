package org.sunbird.enrolments

import akka.actor.ActorRef
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.collections4.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.sunbird.cache.util.RedisCacheUtil
import org.sunbird.common.{CassandraUtil, Constants}
import org.sunbird.common.exception.ProjectCommonException
import org.sunbird.common.models.response.Response
import org.sunbird.common.models.util.ProjectUtil.{EnrolmentType, getConfigValue}
import org.sunbird.common.models.util._
import org.sunbird.common.request.{Request, RequestContext}
import org.sunbird.common.responsecode.ResponseCode
import org.sunbird.helper.ServiceFactory
import org.sunbird.kafka.client.{InstructionEventGenerator, KafkaClient}
import org.sunbird.learner.actors.course.dao.impl.ContentHierarchyDaoImpl
import org.sunbird.learner.actors.coursebatch.dao.impl.{BatchUserDaoImpl, CourseBatchDaoImpl, UserCoursesDaoImpl}
import org.sunbird.learner.actors.coursebatch.dao.{BatchUserDao, CourseBatchDao, UserCoursesDao}
import org.sunbird.learner.util.{BatchCacheHandlerV2, ContentCacheHandlerV2, ContentUtil, ExtendedUtil, JsonUtil, Util}
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
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters._

class ExtendedCourseEnrollmentActor @Inject()(@Named("course-batch-notification-actor") courseBatchNotificationActorRef: ActorRef)(implicit val cacheUtil: RedisCacheUtil)
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
  val dateFormatter = ProjectUtil.getDateFormatter

  dateFormatter.setTimeZone(
    TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)))

  override def preStart { println("Starting ExtendedCourseEnrollmentActor") }

  override def postStop {
    cacheUtil.closePool()
    println("ExtendedCourseEnrollmentActor stopped successfully")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println(s"Restarting ExtendedCourseEnrollmentActor: $message")
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

  override def onReceive(request: Request): Unit = {
    Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass.getName)

    request.getOperation match {
      case "enrollV2" => enroll(request)
      case "list" => list(request)
      case "privateList" => privateList(request)
      case "enrolmentInfoStats" => enrolmentInfoStats(request)
      case "enrolV3Details" => enrolV3Details(request)
      case "enrolProgramV2" => enrollProgram(request)
      case "enrolBlendedProgramV2" => enrollBlendedProgram(request)
      case "bulkEnrolProgramV3" => bulkEnrolProgramV3(request)
      case "enrolDetailsWithProgress" => enrolDetailsWithProgress(request)
      case _ => ProjectCommonException.throwClientErrorException(ResponseCode.invalidRequestData,
        ResponseCode.invalidRequestData.getErrorMessage)
    }
  }

  def enroll(request: Request): Unit = {
    val courseId = request.get(JsonKey.COURSE_ID).asInstanceOf[String]
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    val batchId = request.get(JsonKey.BATCH_ID).asInstanceOf[String]
    val recentLangOpt = Option(request.get(JsonKey.RECENT_LANGUAGE).asInstanceOf[String])

    logger.info(request.getRequestContext, s"ExtendedCourseEnrolmentActor :: enrollWithLanguage :: Request received for courseId=$courseId, userId=$userId, batchId=$batchId, recentLanguage=$recentLangOpt")

    val fieldList = List(JsonKey.PRIMARYCATEGORY, JsonKey.IDENTIFIER, JsonKey.BATCHES)
    val contentData = getContentReadAPIData(courseId, fieldList, request)
    logger.info(request.getRequestContext,
      s"Content metadata fetched | contentDataEmpty=${contentData.isEmpty}")

    if (contentData.isEmpty || !util.Arrays.asList(getConfigValue(JsonKey.COURSE_ENROLL_ALLOWED_PRIMARY_CATEGORY).split(","): _*).contains(contentData.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String]))
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, courseId);

    // Validate batch access
    val batchData: CourseBatch = courseBatchDao.readById(courseId, batchId, request.getRequestContext)
    var enrolmentData: util.List[UserCourses] = userCoursesDao.extendedReadV2(request.getRequestContext, userId, courseId)
    if (CollectionUtils.isEmpty(enrolmentData)) enrolmentData = new util.ArrayList[UserCourses]()

    val batchUserData: BatchUser = batchUserDao.read(request.getRequestContext, batchId, userId)
    validateEnrolmentV3(batchData, enrolmentData, true)

    val dataBatch = createBatchUserMapping(batchId, userId, batchUserData)
    val existingEnrolmentForTheBatch = enrolmentData.asScala.find(_.getBatchId == batchId).orNull
    val recentLang: String = recentLangOpt.getOrElse("")
    val requestId: String = request.getContext.getOrDefault(JsonKey.REQUEST_ID, "").asInstanceOf[String]
    val data: java.util.Map[String, AnyRef] = createUserEnrolmentMap(userId, courseId, batchId, existingEnrolmentForTheBatch, requestId, request.getRequestContext,recentLang)

    val hasAccess = ContentUtil.getContentRead(courseId, request.getContext.getOrDefault(JsonKey.HEADER, new util.HashMap[String, String]).asInstanceOf[util.Map[String, String]])
    if (hasAccess) {
      upsertEnrollment(userId, courseId, batchId, data, dataBatch, existingEnrolmentForTheBatch == null, request.getRequestContext)
      cacheUtil.delete(getCacheKey(userId))
      sender().tell(successResponse(), self)
      logger.info(request.getRequestContext,
        s"Enrollment successful | courseId=$courseId, batchId=$batchId, userId=$userId")

      // Telemetry and notification
      generateTelemetryAudit(userId, courseId, batchId, data, "enrol", JsonKey.CREATE, request.getContext)
      val recentLanguage = data.getOrDefault(JsonKey.RECENT_LANGUAGE, "").asInstanceOf[String]
      notifyUser(userId, batchData, JsonKey.ADD, recentLanguage)
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
    logger.info(request.getRequestContext,"ExtendedCourseEnrollmentActor :: list :: UserId = " + userId)
    try{
      val response = getEnrolmentList(request, userId, false, false)
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : user ::" + userId + "| Exception is:"+e.getMessage, e)
        throw e
    }
  }

  def privateList(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    logger.info(request.getRequestContext, "CourseEnrolmentActorV3 :: list :: UserId = " + userId)
    val activeEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getActiveEnrollments(userId, request)
    val externalEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getExternalEnrollments(userId, request)
    val allEnrolledCourses = new java.util.ArrayList[java.util.Map[String, AnyRef]]
    isRetiredCoursesIncludedInEnrolList = true
    val enrolmentList: java.util.List[java.util.Map[String, AnyRef]] = addCourseDetails_v2(activeEnrolments, true)
    val updatedEnrolmentList = updateProgressData(enrolmentList, request.getRequestContext)
    if (CollectionUtils.isNotEmpty(updatedEnrolmentList)) {
      allEnrolledCourses.addAll(updatedEnrolmentList)
    }
    val userCourseEnrolmentInfo = getUserEnrolmentCourseInfo(allEnrolledCourses.asScala.toList, request, userId);
    var externalCourseInfo = new util.HashMap[String, AnyRef]()
    val allExtEnrolledCourses = new java.util.ArrayList[java.util.Map[String, AnyRef]]
    if (CollectionUtils.isNotEmpty(externalEnrolments)) {
      val externalEnrolmentList: java.util.List[java.util.Map[String, AnyRef]] = addExternalCourseDetails(externalEnrolments, false)
      allExtEnrolledCourses.addAll(addExternalCourseDetails(externalEnrolments, false))
      externalCourseInfo = getUserEnrolmentExternalCourseInfo(externalEnrolmentList.asScala.toList, request)
    }
    try {
      val resp: Response = new Response()
      resp.put(JsonKey.USER_COURSE_ENROLMENT_INFO, userCourseEnrolmentInfo)
      resp.put(JsonKey.USER_COURSE_EXTERNAL_ENROLMENT_INFO, externalCourseInfo)
      resp.put(JsonKey.COURSES, updatedEnrolmentList)
      resp.put(JsonKey.EXTERNAL_COURSES, externalEnrolments)
      sender().tell(resp, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list : user ::" + userId + "| Exception is:" + e.getMessage, e)
        throw e
    }
  }

  def enrolmentInfoStats(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    logger.info(request.getRequestContext,"enrolmentInfoStats :: list :: UserId = " + userId)
    val activeEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getActiveEnrollments(userId, request)
    val externalEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getExternalEnrollments(userId, request)
    val allEnrolledCourses = new java.util.ArrayList[java.util.Map[String, AnyRef]]
    isRetiredCoursesIncludedInEnrolList = true
    val enrolmentList: java.util.List[java.util.Map[String, AnyRef]] = addCourseDetails_v2(activeEnrolments, false)
    if (CollectionUtils.isNotEmpty(enrolmentList)) {
      allEnrolledCourses.addAll(enrolmentList)
    }
    val userCourseEnrolmentInfo = getUserEnrolmentCourseInfo(allEnrolledCourses.asScala.toList, request, userId);
    var externalCourseInfo = new util.HashMap[String, AnyRef]()
    if (CollectionUtils.isNotEmpty(externalEnrolments)) {
      val externalEnrolmentList: java.util.List[java.util.Map[String, AnyRef]] = addExternalCourseDetails(externalEnrolments, false)
      externalCourseInfo = getUserEnrolmentExternalCourseInfo(externalEnrolmentList.asScala.toList, request)
    }
    try {
      val resp: Response = new Response()
      resp.put(JsonKey.USER_COURSE_ENROLMENT_INFO, userCourseEnrolmentInfo)
      resp.put(JsonKey.USER_COURSE_EXTERNAL_ENROLMENT_INFO, externalCourseInfo)
      sender().tell(resp, self)
    }catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list : user ::" + userId + "| Exception is:"+e.getMessage, e)
        throw e
    }
  }

  def enrolV3Details(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    logger.info(request.getRequestContext,"ExtendedCourseEnrollmentActor :: list :: UserId = " + userId)
    try{
      val response = getEnrolmentList(request, userId, true, false)
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : user ::" + userId + "| Exception is:"+e.getMessage, e)
        throw e
    }
  }

  def getEnrolmentList(request: Request, userId: String, isDetailsRequired: Boolean, isProgressEnabled: Boolean): Response = {
    logger.info(request.getRequestContext,"ExtendedCourseEnrollmentActor :: getEnrolmentList :: fetching data from cassandra with userId " + userId)

    val activeEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getActiveEnrollments(userId, request)
    var isMoreThanOneCourse: Boolean = false
    if (request.get(Constants.COURSE_ID) != null) {
      val courseIdListFromRequest = request.get(Constants.COURSE_ID).asInstanceOf[java.util.List[String]]
      if (courseIdListFromRequest.size() > 1) {
        isMoreThanOneCourse = true
      }
    }
    val allEnrolledCourses = new java.util.ArrayList[java.util.Map[String, AnyRef]]
    if (CollectionUtils.isNotEmpty(activeEnrolments)) {
      val enrolmentList: java.util.List[java.util.Map[String, AnyRef]] = addCourseDetails_v2(activeEnrolments, isDetailsRequired)
      val updatedEnrolmentList = updateProgressData(enrolmentList, request.getRequestContext)
      if (isDetailsRequired && !isMoreThanOneCourse) {
        addBatchDetails(updatedEnrolmentList, request, "v3")
        for (enrolment <- updatedEnrolmentList.asScala) {
          if (
            isProgressEnabled &&
              !enrolment.get(JsonKey.STATUS).equals(2)
          ) {
            val courseId = enrolment.get(JsonKey.COURSE_ID).asInstanceOf[String]
            val recentLanguage = enrolment.get(JsonKey.RECENT_LANGUAGE).asInstanceOf[String]
            val batchId = enrolment.get(JsonKey.BATCH_ID).asInstanceOf[String]
            val langContentStatus = Option(enrolment.get("langContentStatus"))
              .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
              .getOrElse(new java.util.HashMap[String, AnyRef]())

            if (
              StringUtils.isNotBlank(recentLanguage) &&
                langContentStatus != null &&
                !langContentStatus.isEmpty &&
                langContentStatus.containsKey(recentLanguage)
            ) {
              val contentIds = Option(langContentStatus.get(recentLanguage))
                .map(_.asInstanceOf[java.util.Map[String, AnyRef]].keySet().asScala.toList.asJava)
                .getOrElse(new java.util.ArrayList[String]())

              if (!contentIds.isEmpty) {
                getConsumption(request, userId, courseId, batchId, contentIds, recentLanguage, enrolment)
              }
            }
          }
        }
      }
      allEnrolledCourses.addAll(updatedEnrolmentList)
    }
    val resp: Response = new Response()
    resp.put(JsonKey.COURSES, allEnrolledCourses)
    resp
  }

  def getActiveEnrollments(userId: String, request: Request): java.util.List[java.util.Map[String, AnyRef]] = {
    isRetiredCoursesIncludedInEnrolList = if (request.get(JsonKey.RETIRED_COURE_ENABLED) != null)
      request.get(JsonKey.RETIRED_COURE_ENABLED).asInstanceOf[Boolean] else false
    val courseIdList:  java.util.List[String] = new java.util.ArrayList()
    var enrolments: java.util.List[java.util.Map[String, AnyRef]] = new java.util.ArrayList()
    var batchId: String = null
    if (request.get(Constants.COURSE_ID) != null) {
      val courseIdListFromRequest = request.get(Constants.COURSE_ID).asInstanceOf[java.util.List[String]]
      courseIdList.addAll(courseIdListFromRequest)
      if (courseIdListFromRequest.size() == 1) {
        val courseEnrolment: java.util.List[java.util.Map[String, AnyRef]] =  userCoursesDao.listEnrolments_v2(request.getRequestContext, userId, courseIdListFromRequest);
        if (CollectionUtils.isEmpty(courseEnrolment)) {
          return new util.ArrayList[java.util.Map[String, AnyRef]]()
        }
        enrichCourseIdFromProgram(request, courseIdList)
      }
    }
    if (request.get(Constants.BATCH_ID) != null) {
      batchId = request.get(Constants.BATCH_ID).asInstanceOf[String]
    }
    if (CollectionUtils.isNotEmpty(courseIdList)) {
      enrolments = userCoursesDao.listEnrolments_v2(request.getRequestContext, userId, courseIdList);
      if (StringUtils.isNotBlank(batchId)) {
        enrolments = userCoursesDao.getEnrolmentByBatchIdAndCourseId_v2(request.getRequestContext, userId, courseIdList.get(0), batchId);
      }
    } else {
      enrolments = userCoursesDao.listEnrolments_v2(request.getRequestContext, userId, null);
    }

    val status: Array[String] = request.get(JsonKey.STATUS) match {
      case arr: Array[String] => arr
      case list: java.util.List[String] => list.toArray(new Array[String](list.size()))
      case str: String => Array(str)
      case _ => null
    }

    val statusFilteredEnrolments = scala.collection.mutable.ArrayBuffer[java.util.List[java.util.Map[String, AnyRef]]]()

    if (CollectionUtils.isNotEmpty(enrolments)) {
      enrolments = enrolments.filter(e => e.getOrDefault(JsonKey.ACTIVE, false.asInstanceOf[AnyRef]).asInstanceOf[Boolean]).toList.asJava
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

      var limit: Integer = if (request.get(JsonKey.LIMIT) != null)  request.get(JsonKey.LIMIT).asInstanceOf[Integer] else -1
      if (limit > -1 && limit !=0) {
        val maximumAllowedLimit = Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.MAXIMUM_LIMIT_ALLOWED_FOR_ENROL_LIST));
        if (maximumAllowedLimit < limit) {
          limit = maximumAllowedLimit
        }
        val sortedEnrolment = enrolments.filter(ae => ae.get("lastContentAccessTime")!=null).toList.sortBy(_.get("lastContentAccessTime").asInstanceOf[Date])(Ordering[Date].reverse).toList
        val finalEnrolments = sortedEnrolment ++ enrolments.asScala.filter(e => e.get("lastContentAccessTime")==null).toList
        if (finalEnrolments.size > limit) {
          enrolments = finalEnrolments.subList(0, limit)
        }
      }
      enrolments
    } else
      new util.ArrayList[java.util.Map[String, AnyRef]]()
  }

  def getUserEnrolmentCourseInfo(finalEnrolment: List[util.Map[String, AnyRef]], actorMessage: Request, userId: String) = {
    var certificateIssued: Int = 0
    var coursesInProgress: Int = 0
    var hoursSpentOnCompletedCourses: Int = 0
    var addInfo: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    finalEnrolment.foreach { courseDetails =>
      val courseStatus = courseDetails.get(JsonKey.STATUS)
      val courseContent: java.util.HashMap[String, AnyRef] = courseDetails.get(JsonKey.CONTENT).asInstanceOf[java.util.HashMap[String, AnyRef]]
      if (courseStatus != 2) {
        if (JsonKey.LIVE.equalsIgnoreCase(courseContent.get(JsonKey.STATUS).asInstanceOf[String])) {
          coursesInProgress += 1
        }
      } else {
        var hoursSpentOnCourses: Int = 0
        val certificatesIssue: java.util.ArrayList[util.Map[String, AnyRef]] = courseDetails.get(JsonKey.ISSUED_CERTIFICATES).asInstanceOf[java.util.ArrayList[util.Map[String, AnyRef]]]
        if (certificatesIssue.nonEmpty) {
          if (null != courseContent.get(JsonKey.DURATION)) {
            hoursSpentOnCourses = courseContent.get(JsonKey.DURATION).asInstanceOf[String].toInt
          }
          hoursSpentOnCompletedCourses += hoursSpentOnCourses
          certificateIssued += 1
        }
      }
    }
    val userKarmaPoints = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
      actorMessage.getRequestContext,
      pageDbInfo.getKeySpace,
      pageDbInfo.getTableName,
      JsonKey.USER_ID,
      userId,
      util.Arrays.asList(JsonKey.USER_KARMA_TOTAL_POINTS, JsonKey.ADD_INFO)
    )
    //dbResponse is a list of maps to extract points for each record
    val dbResponse: java.util.List[util.Map[String, AnyRef]] = userKarmaPoints.get(JsonKey.RESPONSE).asInstanceOf[java.util.List[util.Map[String, AnyRef]]]
    val totalUserKarmaPoints: Int = dbResponse.asScala.collectFirst {
      case record: util.Map[String, AnyRef] if record.containsKey(JsonKey.USER_KARMA_TOTAL_POINTS) =>
        record.get(JsonKey.USER_KARMA_TOTAL_POINTS).asInstanceOf[Integer].toInt
    }.getOrElse(0)
    val addInfoString: String = if (dbResponse.isEmpty) {
      ""
    } else {
      Option(dbResponse.get(0)).flatMap(record => Option(record.get(JsonKey.ADD_INFO)).collect { case str: String => str }).getOrElse("")
    }
    if (addInfoString != null && addInfoString.nonEmpty) {
      val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
      addInfo = objectMapper.readValue(addInfoString, classOf[util.Map[String, AnyRef]])
    }
    val enrolmentCourseDetails = new util.HashMap[String, AnyRef]()
    enrolmentCourseDetails.put(JsonKey.TIME_SPENT_ON_COMPLETED_COURSES, hoursSpentOnCompletedCourses.asInstanceOf[AnyRef])
    enrolmentCourseDetails.put(JsonKey.CERITFICATES_ISSUED, certificateIssued.asInstanceOf[AnyRef])
    enrolmentCourseDetails.put(JsonKey.COURSES_IN_PROGRESS, coursesInProgress.asInstanceOf[AnyRef])
    enrolmentCourseDetails.put(JsonKey.KARMA_POINTS, totalUserKarmaPoints.asInstanceOf[AnyRef])
    enrolmentCourseDetails.put(JsonKey.ADD_INFO, addInfo.asInstanceOf[AnyRef])
    enrolmentCourseDetails
  }

  def getUserEnrolmentExternalCourseInfo(externalEnrolmentFinalEnrolment: List[util.Map[String, AnyRef]], actorMessage: Request) = {
    var certificateIssued: Int = 0
    var coursesInProgress: Int = 0
    var hoursSpentOnCompletedCourses: Int = 0
    externalEnrolmentFinalEnrolment.foreach { courseDetails =>
      val courseStatus = courseDetails.get(JsonKey.STATUS)
      if (courseStatus != 2) {
        coursesInProgress += 1
      } else {
        val courseContent: java.util.HashMap[String, AnyRef] = courseDetails.get(JsonKey.CONTENT).asInstanceOf[java.util.HashMap[String, AnyRef]]
        var hoursSpentOnCourses: Int = 0
        if (MapUtils.isNotEmpty(courseContent)) {
          if (null != courseContent.get(JsonKey.DURATION)) {
            val durationValue = courseContent.get(JsonKey.DURATION).asInstanceOf[String]
            hoursSpentOnCourses = try {
              durationValue.toInt
            } catch {
              case _: NumberFormatException =>
                println(s"Invalid duration value: $durationValue") // Log the invalid value
                0
            }
          }
        }
        hoursSpentOnCompletedCourses += hoursSpentOnCourses
        val certificatesIssue: java.util.ArrayList[util.Map[String, AnyRef]] = courseDetails.get(JsonKey.ISSUED_CERTIFICATES).asInstanceOf[java.util.ArrayList[util.Map[String, AnyRef]]]
        if (certificatesIssue.nonEmpty) {
          certificateIssued += 1
        }
      }
    }
    val enrolmentCourseDetails = new util.HashMap[String, AnyRef]()
    enrolmentCourseDetails.put(JsonKey.TIME_SPENT_ON_COMPLETED_COURSES, hoursSpentOnCompletedCourses.asInstanceOf[AnyRef])
    enrolmentCourseDetails.put(JsonKey.CERITFICATES_ISSUED, certificateIssued.asInstanceOf[AnyRef])
    enrolmentCourseDetails.put(JsonKey.COURSES_IN_PROGRESS, coursesInProgress.asInstanceOf[AnyRef])
    enrolmentCourseDetails
  }

  def addCourseDetails_v2(activeEnrolments: java.util.List[java.util.Map[String, AnyRef]], isDetailsRequired: Boolean): java.util.List[java.util.Map[String, AnyRef]] = {
    activeEnrolments.filter(enrolment => isCourseEligible(enrolment)).map(enrolment => {
      val courseContent = getCourseContent(enrolment.get(JsonKey.COURSE_ID).asInstanceOf[String])
      enrolment.put(JsonKey.LEAF_NODE_COUNT, courseContent.get(JsonKey.LEAF_NODE_COUNT))
      if (isDetailsRequired) {
        enrolment.put(JsonKey.COURSE_NAME, courseContent.get(JsonKey.NAME))
        enrolment.put(JsonKey.DESCRIPTION, courseContent.get(JsonKey.DESCRIPTION))
        enrolment.put(JsonKey.COURSE_LOGO_URL, courseContent.get(JsonKey.APP_ICON))
        enrolment.put(JsonKey.CONTENT_ID, enrolment.get(JsonKey.COURSE_ID))
        enrolment.put(JsonKey.COLLECTION_ID, enrolment.get(JsonKey.COURSE_ID))
      }
      enrolment.put(JsonKey.CONTENT, courseContent)
      enrolment
    }).toList.asJava
  }

  def addExternalCourseDetails(activeEnrolments: java.util.List[java.util.Map[String, AnyRef]], isDetailsRequired: Boolean): java.util.List[java.util.Map[String, AnyRef]] = {
    activeEnrolments.filter(enrolment => isExternalCourseEligible(enrolment)).map(enrolment => {
      val courseContent = getCourseContent(enrolment.get(JsonKey.COURSE_ID).asInstanceOf[String])
      enrolment.put(JsonKey.CONTENT, courseContent)
      enrolment
    }).toList.asJava
  }

  def isCourseEligible(enrolment: java.util.Map[String, AnyRef]): Boolean = {
    val courseContent = getCourseContent(enrolment.get(JsonKey.COURSE_ID).asInstanceOf[String])
    if (null == courseContent || (!JsonKey.LIVE.equalsIgnoreCase(courseContent.get(JsonKey.STATUS).asInstanceOf[String])
      && !isRetiredCoursesIncludedInEnrolList)) {
      logger.info(null,"ExtendedCourseEnrollmentActor :: isCourseEligible :: Failed to fetch data from cache for courseId " + enrolment.get(JsonKey.COURSE_ID).asInstanceOf[String])
      false
    }
    else {
      true
    }
  }

  def isExternalCourseEligible(enrolment: java.util.Map[String, AnyRef]): Boolean = {
    val courseContent = getExternalCourseContent(enrolment.get(JsonKey.COURSE_ID).asInstanceOf[String])
    if (null == courseContent || ((!courseContent.get(JsonKey.IS_ACTIVE).asInstanceOf[Boolean])
      && !isRetiredCoursesIncludedInEnrolList)) {
      false
    }
    else {
      true
    }
  }

  def getCourseContent(courseId: String): java.util.Map[String, AnyRef] = {
    ContentCacheHandlerV2.getInstance().getContent(courseId)
  }

  def getExternalCourseContent(courseId: String): java.util.Map[String, AnyRef] = {
    ContentCacheHandlerV2.getInstance().getExternalContent(courseId)
  }

  def addBatchDetails(enrolmentList: util.List[util.Map[String, AnyRef]], request: Request, version: String): util.List[util.Map[String, AnyRef]] = {

    val batchIds: java.util.List[String] = enrolmentList
      .map(e => e.getOrDefault(JsonKey.BATCH_ID, "").asInstanceOf[String])
      .distinct
      .filter(id => StringUtils.isNotBlank(id))
      .toList
      .asJava

    val batchDetails = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
    val searchIdentifierMaxSize = Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.SEARCH_IDENTIFIER_MAX_SIZE))

    if (JsonKey.VERSION_3.equalsIgnoreCase(version) &&
      JsonKey.TRUE.equalsIgnoreCase(ProjectUtil.getConfigValue(JsonKey.ENROLLMENT_LIST_CACHE_BATCH_FETCH_ENABLED))) {

      logger.info(request.getRequestContext, "Retrieving batch details from the local cache")

      for (enrolment <- enrolmentList.asScala) {
        val batchId = enrolment.getOrDefault(JsonKey.BATCH_ID, "").asInstanceOf[String]
        val courseId = enrolment.getOrDefault(JsonKey.COURSE_ID, "").asInstanceOf[String]
        if (StringUtils.isNotBlank(batchId) && StringUtils.isNotBlank(courseId)) {
          batchDetails.add(getBatchFrmLocalCacheV2(batchId, courseId))
        }
      }

    } else if (batchIds.size() > searchIdentifierMaxSize) {
      for (i <- 0 to batchIds.size() by searchIdentifierMaxSize) {
        val batchIdsSubList: java.util.List[String] = batchIds.subList(i, Math.min(batchIds.size(), i + searchIdentifierMaxSize));
        batchDetails.addAll(searchBatchDetails(batchIdsSubList, request))
      }
    } else {
      batchDetails.addAll(searchBatchDetails(batchIds, request))
    }
    if (CollectionUtils.isNotEmpty(batchDetails)) {
      val batchMap = batchDetails.map(b => b.get(JsonKey.BATCH_ID).asInstanceOf[String] -> b).toMap
      enrolmentList.map(enrolment => {
        enrolment.put(JsonKey.BATCH, batchMap.getOrElse(enrolment.get(JsonKey.BATCH_ID).asInstanceOf[String], new java.util.HashMap[String, AnyRef]()))
        enrolment
      }).toList.asJava
    } else
      enrolmentList
  }

  def searchBatchDetails(batchIds: java.util.List[String], request: Request): java.util.List[java.util.Map[String, AnyRef]] = {
    val requestedFields: java.util.List[String] = if(null != request.getContext.get(JsonKey.BATCH_DETAILS).asInstanceOf[Array[String]]) request.getContext.get(JsonKey.BATCH_DETAILS).asInstanceOf[Array[String]](0).split(",").toList.asJava else new java.util.ArrayList[String]()
    if(CollectionUtils.isNotEmpty(requestedFields)) {
      val fields = new java.util.ArrayList[String]()
      fields.addAll(requestedFields)
      fields.add(JsonKey.BATCH_ID)
      fields.add(JsonKey.IDENTIFIER)
      getBatches(request.getRequestContext ,new java.util.ArrayList[String](batchIds), fields)
    } else {
      new java.util.ArrayList[util.Map[String, AnyRef]]()
    }
  }

  private def enrichCourseIdFromProgram(request: Request, courseIdList:  java.util.List[String]) = {
    if (CollectionUtils.isNotEmpty(courseIdList) && courseIdList.size() == 1) {
      val courseId = courseIdList.get(0)
      val contentData = getCourseContent(courseId)
      val primaryCategory: String = contentData.get(JsonKey.COURSECATEGORY).asInstanceOf[String]
      if (util.Arrays.asList(getConfigValue(JsonKey.PROGRAM_CHILDREN_COURSES_ALLOWED_PRIMARY_CATEGORY).split(","): _*).contains(primaryCategory)) {
        val redisKey = s"$courseId:$courseId:childrenCourses"
        val childrenNodes: List[String] = cacheUtil.getList(redisKey, redisCollectionIndex)
        if (childrenNodes.nonEmpty) {
          courseIdList.addAll(childrenNodes.asJava)
        } else {
          val contentDataForProgram: java.util.List[java.util.Map[String, AnyRef]] = contentHierarchyDao.getContentChildren(request.getRequestContext, courseId)
          if (CollectionUtils.isNotEmpty(contentDataForProgram)) {
            courseIdList.addAll(contentDataForProgram.asScala
              .map(childNode => childNode.get(JsonKey.IDENTIFIER).asInstanceOf[String])
              .asJava
            )
          } else {
            logger.error(request.getRequestContext, "Not able to get the hierarchy for the content with contentId: " + courseId, null)
          }
        }
      } else {
        logger.info(request.getRequestContext, "The primary category is not valid to fetch the children for primaryCategory : " + primaryCategory + " for courseId: " + courseId)
      }
    } else {
      logger.info(request.getRequestContext, "CourseId Not present in request or more than 1 courseId so request is not from TOC page, no enhancement required.")
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

  def enrollProgram(request: Request): Unit = {
    val programId: String = request.get(JsonKey.PROGRAM_ID).asInstanceOf[String]
    val isAdminAPI: Boolean = request.get(JsonKey.IS_ADMIN_API).asInstanceOf[Boolean]
    val fieldList = List(JsonKey.PRIMARYCATEGORY, JsonKey.IDENTIFIER, JsonKey.BATCHES, JsonKey.LANGUAGE)
    val contentData = getContentReadAPIData(programId, fieldList, request)
    // Extract language from contentData and set as recentLanguage in the request
    val languageListOpt = Option(contentData.get(JsonKey.LANGUAGE))
    val recentLanguage = languageListOpt match {
      case Some(langList: java.util.List[_]) if !langList.isEmpty =>
        langList.get(0).toString.toLowerCase
      case _ =>
        throw new ProjectCommonException(
          ResponseCode.invalidParameterValue.getErrorCode,
          JsonKey.LANGUAGE_NOT_FOUND_IN_CONTENT,
          ResponseCode.CLIENT_ERROR.getResponseCode
        )
    }

    request.put(JsonKey.RECENT_LANGUAGE, recentLanguage)
    if (isAdminAPI && (contentData.size() == 0 || !util.Arrays.asList(getConfigValue(JsonKey.ADMIN_PROGRAM_ENROLL_ALLOWED_PRIMARY_CATEGORY).split(","): _*).contains(contentData.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String])))
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, programId);
    if (!isAdminAPI && (contentData.size() == 0 || !util.Arrays.asList(getConfigValue(JsonKey.PROGRAM_ENROLL_ALLOWED_PRIMARY_CATEGORY).split(","): _*).contains(contentData.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String])))
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, programId);
    val userId: String = request.get(JsonKey.USER_ID).asInstanceOf[String]
    val batchId: String = request.get(JsonKey.BATCH_ID).asInstanceOf[String]
    val batchData: CourseBatch = courseBatchDao.readById(programId, batchId, request.getRequestContext)
    val verifyBatchType: Boolean = Option(request.getContext.get("verifyBatchType").asInstanceOf[Boolean]).getOrElse(false)
    if(verifyBatchType && !("open".equalsIgnoreCase(batchData.getEnrollmentType))) {
      ProjectCommonException.throwClientErrorException(ResponseCode.notOpenBatch);
    }
    var enrolmentData: UserCourses = null
    val enrolmentDataList: java.util.List[UserCourses] = userCoursesDao.extendedReadAllV2(request.getRequestContext, userId, programId)
    if (null != enrolmentDataList) {
      for (enrolment <- enrolmentDataList) {
        if (enrolment.isActive) {
          ProjectCommonException.throwClientErrorException(ResponseCode.userAlreadyEnrolledCourse);
        }
        if (enrolment.getBatchId.equals(batchId)) {
          enrolmentData = enrolment
        }
      }
    }
    val batchUserData: BatchUser = batchUserDao.read(request.getRequestContext, batchId, userId)
    val primaryCategory=contentData.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String]
    if(primaryCategory.equalsIgnoreCase(JsonKey.STANDALONE_ASSESSMENT)) {
      validateEnrolmentV2(batchData, enrolmentData, true,primaryCategory)
    }
    validateEnrolment(batchData, enrolmentData, true)
    getCoursesForProgramAndEnrol(request, programId, userId, batchId)
    val dataBatch: util.Map[String, AnyRef] = createBatchUserMapping(batchId, userId, batchUserData)
    val data: java.util.Map[String, AnyRef] = createUserEnrolmentMap(userId, programId, batchId, enrolmentData, request.getContext.getOrDefault(JsonKey.REQUEST_ID, "").asInstanceOf[String], request.getRequestContext,recentLanguage)
    upsertEnrollment(userId, programId, batchId, data, dataBatch, (null == enrolmentData), request.getRequestContext)
    logger.info(request.getRequestContext, "ProgramEnrolmentActor :: enroll :: Deleting redis for key " + getCacheKey(userId))
    cacheUtil.delete(getCacheKey(userId))
    generatePreProcessorKafkaEvent(request,batchId, programId, userId)
    sender().tell(successResponse(), self)
    generateTelemetryAudit(userId, programId, batchId, data, "enrol", JsonKey.CREATE, request.getContext)

    notifyUser(userId, batchData, JsonKey.ADD, recentLanguage)
    cacheUtil.delete(getCacheBatchKey(batchId))
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

  def enrollBlendedProgram(request: Request): Unit = {
    val courseId: String = request.get(JsonKey.COURSE_ID).asInstanceOf[String]
    val userId: String = request.get(JsonKey.USER_ID).asInstanceOf[String]
    val batchId: String = request.get(JsonKey.BATCH_ID).asInstanceOf[String]
    logger.info(request.asInstanceOf[Request].getRequestContext, "CourseEnrolmentActor Request for enroll recieved, UserId : " + userId + ", courseId : " + courseId +", batchId : "+batchId)
    val fieldList = List(JsonKey.PRIMARYCATEGORY, JsonKey.IDENTIFIER, JsonKey.BATCHES, JsonKey.LANGUAGE)
    val contentData = getContentReadAPIData(courseId, fieldList, request)
    if (!courseAllowedPrimaryCategories.contains(contentData.getOrDefault(JsonKey.PRIMARYCATEGORY, "").asInstanceOf[String]))
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, courseId);

    val languageListOpt = Option(contentData.get(JsonKey.LANGUAGE))
    val courseLanguage = languageListOpt match {
      case Some(langList: java.util.List[_]) if !langList.isEmpty =>
        langList.get(0).toString.toLowerCase
      case _ =>
        throw new ProjectCommonException(
          ResponseCode.invalidParameterValue.getErrorCode,
          JsonKey.LANGUAGE_NOT_FOUND_IN_CONTENT,
          ResponseCode.CLIENT_ERROR.getResponseCode
        )
    }
    request.put(JsonKey.RECENT_LANGUAGE, courseLanguage)
    val batchData: CourseBatch = courseBatchDao.readById( courseId, batchId, request.getRequestContext)
    val enrolmentData: UserCourses = userCoursesDao.read(request.getRequestContext, userId, courseId, batchId)
    val batchUserData: BatchUser = batchUserDao.read(request.getRequestContext, batchId, userId)
    validateEnrolment(batchData, enrolmentData, true, true)
    val dataBatch: util.Map[String, AnyRef] = createBatchUserMapping(batchId, userId,batchUserData)
    val data: java.util.Map[String, AnyRef] = createUserEnrolmentMap(userId, courseId, batchId, enrolmentData, request.getContext.getOrDefault(JsonKey.REQUEST_ID, "").asInstanceOf[String], request.getRequestContext, courseLanguage)
    val dateTimeFormat = ProjectUtil.getDateFormatter()
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)))
    val enrolledDate = new SimpleDateFormat(Constants.SIMPLE_DATE_FORMAT).parse(request.get(JsonKey.ENROLLED_DATE).asInstanceOf[String])
    val enrolledTimestamp = new java.sql.Timestamp(dateTimeFormat.parse(dateTimeFormat.format(enrolledDate)).getTime())
    dataBatch.put(JsonKey.COURSE_ENROLL_DATE, enrolledTimestamp)
    data.put(JsonKey.COURSE_ENROLL_DATE, enrolledTimestamp)
    val hasAccess = ContentUtil.getContentRead(courseId, request.getContext.getOrDefault(JsonKey.HEADER, new util.HashMap[String, String]).asInstanceOf[util.Map[String, String]])
    if (hasAccess) {
      //Enrolling into children course if any
      getCoursesForProgramAndEnrol(request, courseId, userId, batchId)
      upsertEnrollment(userId, courseId, batchId, data, dataBatch, (null == enrolmentData), request.getRequestContext)
      logger.info(request.getRequestContext, "CourseEnrolmentActor :: enroll :: Deleting redis for key " + getCacheKey(userId))
      cacheUtil.delete(getCacheKey(userId))
      sender().tell(successResponse(), self)
      generateTelemetryAudit(userId, courseId, batchId, data, "enrol", JsonKey.CREATE, request.getContext)
      notifyUser(userId, batchData, JsonKey.ADD, courseLanguage)
      val dataMap = new java.util.HashMap[String, AnyRef]
      val requestMap = new java.util.HashMap[String, AnyRef]
      requestMap.put(JsonKey.COURSE_ID,courseId)
      requestMap.put(JsonKey.USER_ID,userId)
      requestMap.put(JsonKey.BATCH_ID,batchId)
      dataMap.put("edata",requestMap)
      val topic = ProjectUtil.getConfigValue("kafka_user_enrolment_event_topic")
      InstructionEventGenerator.createCourseEnrolmentEvent("", topic, dataMap)
      cacheUtil.delete(getCacheBatchKey(batchId))
    } else {
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, courseId)
    }
  }

  def bulkEnrolProgramV3(request: Request): Unit = {
    val response: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    val status: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    val resp: Response = new Response()
    val programId: String = request.get(JsonKey.PROGRAM_ID).asInstanceOf[String]
    val isAdminAPI: Boolean = request.get(JsonKey.IS_ADMIN_API).asInstanceOf[Boolean]
    val fieldList = List(JsonKey.PRIMARYCATEGORY, JsonKey.IDENTIFIER, JsonKey.BATCHES, JsonKey.LANGUAGE)
    val contentData = getContentReadAPIData(programId, fieldList, request)

    if (isAdminAPI && !adminAllowedPrimaryCategories.contains(contentData.getOrDefault(JsonKey.PRIMARYCATEGORY, "").asInstanceOf[String]))
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, programId);

    if (!isAdminAPI && !programAllowedPrimaryCategories.contains(contentData.getOrDefault(JsonKey.PRIMARYCATEGORY, "").asInstanceOf[String]))
      ProjectCommonException.throwClientErrorException(ResponseCode.accessDeniedToEnrolOrUnenrolCourse, programId);

    val languageListOpt = Option(contentData.get(JsonKey.LANGUAGE))
    val courseLanguage = languageListOpt match {
      case Some(langList: java.util.List[_]) if !langList.isEmpty =>
        langList.get(0).toString.toLowerCase
      case _ =>
        throw new ProjectCommonException(
          ResponseCode.invalidParameterValue.getErrorCode,
          JsonKey.LANGUAGE_NOT_FOUND_IN_CONTENT,
          ResponseCode.CLIENT_ERROR.getResponseCode
        )
    }
    request.put(JsonKey.RECENT_LANGUAGE, courseLanguage)

    val userIds = request.get(JsonKey.USERID_LIST).asInstanceOf[java.util.List[String]]
    val batchId: String = request.get(JsonKey.BATCH_ID).asInstanceOf[String]
    val batchData: CourseBatch = courseBatchDao.readById(programId, batchId, request.getRequestContext)
    val enrolledUsers = Option(userCoursesDao.getBatchParticipants(request.getRequestContext, batchId, true))
      .getOrElse(new java.util.ArrayList[Any]())

    val batchAttributesOpt = Option(batchData.getBatchAttributes)
    val maxBatchSizeStr = batchAttributesOpt
      .flatMap(attrs => Option(attrs.get(JsonKey.CURRENT_BATCH_SIZE)))
      .map(_.toString.trim)
      .getOrElse("")

    val courseCategory = Option(contentData.get(JsonKey.COURSECATEGORY))
      .map(_.toString.trim)
      .getOrElse("")

    val isBlendedProgram = courseCategory.equalsIgnoreCase(JsonKey.BLENDED_PROGRAM)
    val isMaxBatchSizeValid = maxBatchSizeStr.nonEmpty && maxBatchSizeStr.forall(_.isDigit)

    if (isBlendedProgram && !isMaxBatchSizeValid) {
      ProjectCommonException.throwClientErrorException(
        ResponseCode.batchSizeNotDefined,
        ResponseCode.batchSizeNotDefined.getErrorMessage
      )
    }

    if (isMaxBatchSizeValid) {
      val maxBatchSize = maxBatchSizeStr.toInt
      val currentSize = enrolledUsers.size() + userIds.size()
      if (currentSize > maxBatchSize) {
        val remainingSlots = maxBatchSize - enrolledUsers.size()
        ProjectCommonException.throwClientErrorException(
          ResponseCode.batchSizeExceeded,
          MessageFormat.format(
            ResponseCode.batchSizeExceeded.getErrorMessage,
            Integer.valueOf(remainingSlots)
          )
        )
      }
    }
    for (userId <- userIds) {
      try {
        var enrolmentData: UserCourses = null
        val enrolmentDataList: java.util.List[UserCourses] = userCoursesDao.extendedReadAllV2(request.getRequestContext, userId, programId)
        if (null != enrolmentDataList) {
          for (enrolment <- enrolmentDataList) {
            if (enrolment.getBatchId.equals(batchId)) {
              if (enrolment.isActive) {
                ProjectCommonException.throwClientErrorException(ResponseCode.userAlreadyEnrolledCourse);
              } else {
                enrolmentData = enrolment;
              }
            } else if (enrolment.isActive) {
              ProjectCommonException.throwClientErrorException(ResponseCode.userAlreadyEnrolledCourseWithDifferentBatch);
            }
          }
        }
        val batchUserData: BatchUser = batchUserDao.read(request.getRequestContext, batchId, userId)
        val primaryCategory=contentData.get(JsonKey.PRIMARYCATEGORY).asInstanceOf[String]
        if(primaryCategory.equalsIgnoreCase(JsonKey.STANDALONE_ASSESSMENT)) {
          validateEnrolmentV2(batchData, enrolmentData, true,primaryCategory)
        }else{
          validateEnrolment(batchData, enrolmentData, true)
        }
        getCoursesForProgramAndEnrol(request, programId, userId, batchId)
        val dataBatch: util.Map[String, AnyRef] = createBatchUserMapping(batchId, userId, batchUserData)
        val data: java.util.Map[String, AnyRef] = createUserEnrolmentMap(userId, programId, batchId, enrolmentData, request.getContext.getOrDefault(JsonKey.REQUEST_ID, "").asInstanceOf[String], request.getRequestContext, courseLanguage)
        upsertEnrollment(userId, programId, batchId, data, dataBatch, (null == enrolmentData), request.getRequestContext)
        logger.info(request.getRequestContext, "ProgramEnrolmentActor :: enroll :: Deleting redis for key " + getCacheKey(userId))
        cacheUtil.delete(getCacheKey(userId))
        generatePreProcessorKafkaEvent(request, batchId, programId, userId)
        generateTelemetryAudit(userId, programId, batchId, data, "enrol", JsonKey.CREATE, request.getContext)
        notifyUser(userId, batchData, JsonKey.ADD, courseLanguage)
        status.put(JsonKey.STATUS, JsonKey.SUCCESS)
        response.put(userId, status)
      } catch {
        case e: ProjectCommonException =>
          if (ResponseCode.userAlreadyEnrolledCourse.getErrorMessage.equals(e.getMessage)) {
            map.put(JsonKey.STATUS, JsonKey.FAILED)
            map.put(JsonKey.ERRORMSG, ResponseCode.userAlreadyEnrolledCourse.getErrorMessage)
            response.put(userId, map)
          } else {
            map.put(JsonKey.STATUS, JsonKey.FAILED)
            map.put(JsonKey.ERRORMSG, e.getMessage)
            response.put(userId, status)
          }
        case e: Exception =>
          map.put(JsonKey.STATUS, JsonKey.FAILED)
          map.put(JsonKey.ERRORMSG, e.getMessage)
          response.put(userId, status)
      }
      cacheUtil.delete(getCacheBatchKey(batchId))
      resp.put(JsonKey.RESPONSE, response)
    }
    sender().tell(resp, self)
  }

  def enrolDetailsWithProgress(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    logger.info(request.getRequestContext, "ExtendedCourseEnrollmentActor :: list :: UserId = " + userId)
    try {
      val response = getEnrolmentList(request, userId, true, true)
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : user ::" + userId + "| Exception is:" + e.getMessage, e)
        throw e
    }
  }

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
    val contentsConsumed = getContentsConsumption(userId, courseId, contentIds, batchId, language, request.getRequestContext)
    if (CollectionUtils.isNotEmpty(contentsConsumed)) {
      val filteredContents = contentsConsumed.map { m =>
        ProjectUtil.removeUnwantedFields(m, JsonKey.DATE_TIME, JsonKey.USER_ID, JsonKey.ADDED_BY, JsonKey.LAST_UPDATED_TIME, JsonKey.OLD_LAST_ACCESS_TIME, JsonKey.OLD_LAST_UPDATED_TIME, JsonKey.OLD_LAST_COMPLETED_TIME)
        m.put(JsonKey.COLLECTION_ID, m.getOrDefault(JsonKey.COURSE_ID, ""))
        jsonFields.foreach(field =>
          if (m.get(field) != null)
            m.put(field, mapper.readTree(m.get(field).asInstanceOf[String]))
        )
        val formattedMap = JsonUtil.convertWithDateFormat(m, classOf[util.Map[String, Object]], dateFormatter)
        if (fields.contains(JsonKey.ASSESSMENT_SCORE))
          formattedMap.putAll(mapAsJavaMap(Map(JsonKey.ASSESSMENT_SCORE -> getScore(userId, courseId, m.get("contentId").asInstanceOf[String], batchId, request.getRequestContext))))
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

}
