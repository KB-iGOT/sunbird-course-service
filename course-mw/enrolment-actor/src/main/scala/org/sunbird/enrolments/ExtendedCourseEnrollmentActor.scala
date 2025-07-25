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
import org.sunbird.learner.actors.course.dao.impl.ContentHierarchyDaoImpl
import org.sunbird.learner.actors.coursebatch.dao.impl.{BatchUserDaoImpl, CourseBatchDaoImpl, UserCoursesDaoImpl}
import org.sunbird.learner.actors.coursebatch.dao.{BatchUserDao, CourseBatchDao, UserCoursesDao}
import org.sunbird.learner.util.{BatchCacheHandler, ContentCacheHandlerV2, ContentUtil, JsonUtil, Util}
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

  def list(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    logger.info(request.getRequestContext,"ExtendedCourseEnrollmentActor :: list :: UserId = " + userId)
    try{
      val response = getEnrolmentList(request, userId, false)
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : user ::" + userId + "| Exception is:"+e.getMessage, e)
        throw e
    }
  }

  def privateList(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    logger.info(request.getRequestContext, "ExtendedCourseEnrollmentActor :: list :: UserId = " + userId)
    try {
      val response = getEnrolmentList(request, userId, false)
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : user ::" + userId + "| Exception is:" + e.getMessage, e)
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
      val response = getEnrolmentList(request, userId, true)
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : user ::" + userId + "| Exception is:"+e.getMessage, e)
        throw e
    }
  }

  def getEnrolmentList(request: Request, userId: String, isDetailsRequired: Boolean): Response = {
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
        addBatchDetails(updatedEnrolmentList, request,"v3")
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

    if (CollectionUtils.isNotEmpty(enrolments)) {
      enrolments = enrolments.filter(e => e.getOrDefault(JsonKey.ACTIVE, false.asInstanceOf[AnyRef]).asInstanceOf[Boolean]).toList.asJava
      // Map status strings to their integer values, ignoring unknown statuses
      if (status != null) {
        val statusValues: Set[Int] = status.flatMap(s => statusMap.get(s)).toSet
        if (statusValues.nonEmpty) {
          enrolments = enrolments
            .filter(e => statusValues.contains(e.getOrDefault(JsonKey.STATUS, (-1).asInstanceOf[AnyRef]).asInstanceOf[Int]))
            .toList
            .asJava
        }
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

  def addBatchDetails(enrolmentList: util.List[util.Map[String, AnyRef]], request: Request,version:String): util.List[util.Map[String, AnyRef]] = {
    val batchIds:java.util.List[String] = enrolmentList.map(e => e.getOrDefault(JsonKey.BATCH_ID, "").asInstanceOf[String]).distinct.filter(id => StringUtils.isNotBlank(id)).toList.asJava
    val batchDetails = new java.util.ArrayList[java.util.Map[String, AnyRef]]();
    val searchIdentifierMaxSize = Integer.parseInt(ProjectUtil.getConfigValue(JsonKey.SEARCH_IDENTIFIER_MAX_SIZE));
    if (JsonKey.VERSION_3.equalsIgnoreCase(version) &&
      JsonKey.TRUE.equalsIgnoreCase(ProjectUtil.getConfigValue(JsonKey.ENROLLMENT_LIST_CACHE_BATCH_FETCH_ENABLED))){
      logger.info(request.getRequestContext, "Retrieving batch details from the local cache");
      for (i <- 0 to batchIds.size()-1) {
        batchDetails.add(getBatchFrmLocalCache(batchIds.get(i)))
      }
    }
    else if (batchIds.size() > searchIdentifierMaxSize) {
      for (i <- 0 to batchIds.size() by searchIdentifierMaxSize) {
        val batchIdsSubList: java.util.List[String] = batchIds.subList(i, Math.min(batchIds.size(), i + searchIdentifierMaxSize));
        batchDetails.addAll(searchBatchDetails(batchIdsSubList, request))
      }
    } else {
      batchDetails.addAll(searchBatchDetails(batchIds, request))
    }
    if(CollectionUtils.isNotEmpty(batchDetails)){
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

  def getBatchFrmLocalCache(batchId: String): java.util.Map[String, AnyRef] = {
    val batchesMap = BatchCacheHandler.getBatchMap.asInstanceOf[java.util.Map[String, java.util.Map[String, AnyRef]]]
    var batch = batchesMap.get(batchId)
    if (batch == null || batch.size() < 1)
      batch = BatchCacheHandler.getBatch(batchId)
    batch
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
      val leafNodesCount: Int = enrolment.getOrDefault("leafNodesCount", 0.asInstanceOf[AnyRef]).asInstanceOf[Int]
      val progress: Int = enrolment.getOrDefault("progress", 0.asInstanceOf[AnyRef]).asInstanceOf[Int]
      enrolment.put("status", getCompletionStatus(progress, leafNodesCount).asInstanceOf[AnyRef])
      enrolment.put("completionPercentage", getCompletionPerc(progress, leafNodesCount).asInstanceOf[AnyRef])

      jsonFields.foreach { field =>
        if (enrolment.containsKey(field) && null != enrolment.get(field)) {
          enrolment.put(field, mapper.readTree(enrolment.get(field).asInstanceOf[String]))
        } else {
          enrolment.put(field, new java.util.HashMap[String, AnyRef]())
        }
      }

      // New logic: update contentStatus if recentLanguage is present and contentStatus is null
      val recentLanguage = enrolment.get("recent_language")
      val contentStatus = enrolment.get("contentstatus")
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
}
