package org.sunbird.enrolments

import akka.actor.ActorRef
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.collections4.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.sunbird.cache.util.RedisCacheUtil
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
import org.sunbird.learner.util._
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

  def list(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    try {
      val statusFilter = Option(request.get(JsonKey.STATUS))
        .map(_.asInstanceOf[String])
        .orNull
      val savedStatus = request.get(JsonKey.STATUS)
      request.getRequest.remove(JsonKey.STATUS)
      val activeEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getActiveEnrollments(userId, request)
      val externalEnrolments: java.util.List[java.util.Map[String, AnyRef]] = getExternalEnrollments(userId, request)

      if (savedStatus != null) {
        request.getRequest.put(JsonKey.STATUS, savedStatus)
      }
      val badgeStats = getBadgeStats(request, userId, activeEnrolments, statusFilter, isExternal = false)
      val externalBadgeStats = getBadgeStats(request, userId, externalEnrolments, statusFilter, isExternal = true)
      val internalSummary = badgeStats.get(JsonKey.SUMMARY).asInstanceOf[java.util.Map[String, AnyRef]]
      val externalSummary = externalBadgeStats.get(JsonKey.SUMMARY).asInstanceOf[java.util.Map[String, AnyRef]]

      val totalBadgesEarned = internalSummary.get(JsonKey.TOTAL_BADGES_EARNED).asInstanceOf[Integer] +
                              externalSummary.get(JsonKey.TOTAL_BADGES_EARNED).asInstanceOf[Integer]
      val totalCourseCompleted = internalSummary.get(JsonKey.COURSE_COMPLETED).asInstanceOf[Integer] +
                                 externalSummary.get(JsonKey.COURSE_COMPLETED).asInstanceOf[Integer]

      val mergedEarnedBadges = mergeBadgeLists(
        badgeStats.get(JsonKey.EARNED_BADGES_DETAILS).asInstanceOf[java.util.Map[String, AnyRef]],
        externalBadgeStats.get(JsonKey.EARNED_BADGES_DETAILS).asInstanceOf[java.util.Map[String, AnyRef]]
      )

      val mergedInProgressBadges = mergeBadgeLists(
        badgeStats.get(JsonKey.IN_PROGRESS_BADGES_DETAILS).asInstanceOf[java.util.Map[String, AnyRef]],
        externalBadgeStats.get(JsonKey.IN_PROGRESS_BADGES_DETAILS).asInstanceOf[java.util.Map[String, AnyRef]]
      )

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
      mergedSummary.put(JsonKey.IN_PROGRESS_COUNT, inProgressCount.asInstanceOf[AnyRef])

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
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : request ::" + mapper.writeValueAsString(request) + "| Exception is:" + e.getMessage, e)
        throw e
    }
  }

  def getActiveEnrollments(userId: String, request: Request): java.util.List[java.util.Map[String, AnyRef]] = {
    var enrolments: java.util.List[java.util.Map[String, AnyRef]] = new java.util.ArrayList()
    enrolments = userCoursesDao.listEnrolments_v2(request.getRequestContext, userId, null)
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

      enrolments
    } else {
      new util.ArrayList[java.util.Map[String, AnyRef]]()
    }
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

  def getCacheBatchKey(batchId: String) = s"$batchId:active-participants-count"


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

    val now = System.currentTimeMillis()

    // STEP 2: Extract course IDs from enrolments
    val courseIds: java.util.List[String] = enrolments.asScala
      .map(e => e.get(JsonKey.COURSE_ID).asInstanceOf[String])
      .distinct
      .asJava

    if (courseIds.isEmpty) {
      return createEmptyBadgeStatsResponse()
    }

    // STEP 3: Search API — Filter badge courses (with built-in content data)
    val badgeCourseMap: Map[String, (java.util.List[java.util.Map[String, AnyRef]], String, Int)] =
      fetchBadgeCoursesFromSearch(courseIds, request, isExternal)

    // STEP 4: Filter enrolments to only badge courses
    val badgeEnrolments = enrolments.asScala.filter { e =>
      val courseId = e.get(JsonKey.COURSE_ID).asInstanceOf[String]
      badgeCourseMap.contains(courseId)
    }.toList

    if (badgeEnrolments.isEmpty) {
      val emptyResponse = createEmptyBadgeStatsResponse()
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
    result
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
        case _: Exception => 100
      }

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
      val ciosBaseUrl = ProjectUtil.getConfigValue(JsonKey.CB_PORES_SERVICE_BASE_URL)
      val ciosSearchPath = ProjectUtil.getConfigValue(JsonKey.CB_PORES_CIOS_SEARCH_API_URL)
      val ciosSearchUrl = ciosBaseUrl + ciosSearchPath
      logger.info(request.getRequestContext, s"searchExternalContent :: Calling CIOS API at: $ciosSearchUrl")

      val searchRequest = requestBody.get(JsonKey.REQUEST).asInstanceOf[java.util.Map[String, AnyRef]]
      val filters = searchRequest.get(JsonKey.FILTERS).asInstanceOf[java.util.Map[String, AnyRef]]
      val fields = searchRequest.get(JsonKey.FIELDS).asInstanceOf[java.util.List[String]]

      val ciosRequestBody = new java.util.HashMap[String, AnyRef]()

      val filterCriteriaMap = new java.util.HashMap[String, AnyRef]()
      if (filters.containsKey(JsonKey.IDENTIFIER)) {
        val courseIds = filters.get(JsonKey.IDENTIFIER).asInstanceOf[java.util.List[String]]
        filterCriteriaMap.put("contentId", courseIds)
      }
      val badgeFilterKey = s"${JsonKey.BADGE_DETAILS_V1}.${JsonKey.BADGE_EARNING_DATE_ENABLED}"
      if (filters.containsKey(badgeFilterKey)) {
        filterCriteriaMap.put("badgeDetails_v1.badgeEarningDateEnabled", filters.get(badgeFilterKey))
      }
      filterCriteriaMap.put("contentPartner.isActive", java.lang.Boolean.TRUE)

      ciosRequestBody.put("filterCriteriaMap", filterCriteriaMap)

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
      ciosRequestBody.put(JsonKey.REQUESTED_FIELDS, requestedFields)
      val ciosHeaders = new java.util.HashMap[String, String]()
      ciosHeaders.put(JsonKey.CONTENT_TYPE_KEY, JsonKey.APPLICATION_JSON)
      ciosHeaders.put(JsonKey.ACCEPT, "*/*")

      val response = HttpUtil.sendPostRequest(ciosSearchUrl, mapper.writeValueAsString(ciosRequestBody), ciosHeaders)

      if (response != null && response.nonEmpty) {
        val ciosResponse = mapper.readValue(response, classOf[java.util.Map[String, AnyRef]])
        logger.info(request.getRequestContext, s"searchExternalContent :: CIOS API returned response")
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
      val data = Option(ciosResponse.get("data"))
        .orElse(Option(ciosResponse.get("content")))
        .orElse(Option(ciosResponse.get("contents")))
        .collect { case l: java.util.List[java.util.Map[String, AnyRef]] => l }
        .getOrElse(new java.util.ArrayList[java.util.Map[String, AnyRef]]())

      val transformedContents = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
      data.asScala.foreach { item =>
        val transformed = new java.util.HashMap[String, AnyRef]()

        if (item.containsKey("contentId")) {
          transformed.put(JsonKey.IDENTIFIER, item.get("contentId"))
        }

        item.asScala.foreach {
          case ("contentId", _) => // Already mapped to identifier
          case (key, value) => transformed.put(key, value)
        }

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
