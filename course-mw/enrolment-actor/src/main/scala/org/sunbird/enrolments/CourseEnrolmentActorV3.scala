package org.sunbird.enrolments

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.collections4.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.sunbird.cache.util.RedisCacheUtil
import org.sunbird.common.Constants
import org.sunbird.common.exception.ProjectCommonException
import org.sunbird.common.models.response.Response
import org.sunbird.common.models.util.ProjectUtil.getConfigValue
import org.sunbird.common.models.util.{JsonKey, ProjectUtil, TelemetryEnvKey}
import org.sunbird.common.request.{Request, RequestContext}
import org.sunbird.common.responsecode.ResponseCode
import org.sunbird.helper.ServiceFactory
import org.sunbird.learner.actors.course.dao.impl.ContentHierarchyDaoImpl
import org.sunbird.learner.actors.coursebatch.dao.impl.{BatchUserDaoImpl, CourseBatchDaoImpl, UserCoursesDaoImpl}
import org.sunbird.learner.actors.coursebatch.dao.{BatchUserDao, CourseBatchDao, UserCoursesDao}
import org.sunbird.learner.util._

import java.util
import java.util.Date
import javax.inject.Inject
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class CourseEnrolmentActorV3 @Inject()(implicit val  cacheUtil: RedisCacheUtil ) extends BaseEnrolmentActor {

  /*
  The below variables are kept as var on testcase purpose.
  TODO: once all are moved to scala, this can be made as parameterised constructor
   */
  var courseBatchDao: CourseBatchDao = new CourseBatchDaoImpl()
  var userCoursesDao: UserCoursesDao = new UserCoursesDaoImpl()
  var batchUserDao  : BatchUserDao   = new BatchUserDaoImpl()
  var contentHierarchyDao: ContentHierarchyDaoImpl = new ContentHierarchyDaoImpl()
  var isRetiredCoursesIncludedInEnrolList = false
  val ttl: Int = if (StringUtils.isNotBlank(ProjectUtil.getConfigValue("user_enrolments_response_cache_ttl")))
    (ProjectUtil.getConfigValue("user_enrolments_response_cache_ttl")).toInt else 60
  val redisCollectionIndex = if (StringUtils.isNotBlank(ProjectUtil.getConfigValue("redis_collection_index")))
    (ProjectUtil.getConfigValue("redis_collection_index")).toInt else 10
  private val pageDbInfo = Util.dbInfoMap.get(JsonKey.USER_KARMA_POINTS_DB)
  private val externalCourseEnrolDbInfo = Util.dbInfoMap.get(JsonKey.EXTERNAL_COURSES_ENROLMENT_DB)
  private val cassandraOperation = ServiceFactory.getInstance
  val statusMap: Map[String, Int] = Map("In-Progress" -> 1, "Completed" -> 2, "Not-Started" -> 0)
  val jsonFields = Set[String]("lrcProgressDetails")
  private val mapper = new ObjectMapper
  override def preStart { println("Starting CourseEnrolmentActorV3") }

  override def postStop {
    cacheUtil.closePool()
    println("CourseEnrolmentActorV3 stopped successfully")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println(s"Restarting CourseEnrolmentActorV3: $message")
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

  override def onReceive(request: Request): Unit = {
    Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass.getName)

    request.getOperation match {
      case "list" => list(request)
      case "enrolmentInfoStats" => enrolmentInfoStats(request)
      case "enrolV3Details" => enrolV3Details(request)
      case _ => ProjectCommonException.throwClientErrorException(ResponseCode.invalidRequestData,
        ResponseCode.invalidRequestData.getErrorMessage)
    }
  }

  def list(request: Request): Unit = {
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    logger.info(request.getRequestContext,"CourseEnrolmentActorV3 :: list :: UserId = " + userId)
    try{
      val response = getEnrolmentList(request, userId, false)
      sender().tell(response, self)
    } catch {
      case e: Exception =>
        logger.error(request.getRequestContext, "Exception in enrolment list v3 : user ::" + userId + "| Exception is:"+e.getMessage, e)
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
    logger.info(request.getRequestContext,"CourseEnrolmentActorV3 :: list :: UserId = " + userId)
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
    logger.info(request.getRequestContext,"CourseEnrolmentActorV3 :: getEnrolmentList :: fetching data from cassandra with userId " + userId)

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
        enrichCourseIdFromProgram(request, courseIdList)
      }
    }
    if (request.get(Constants.BATCH_ID) != null) {
      batchId = request.get(Constants.BATCH_ID).asInstanceOf[String]
    }
    if (CollectionUtils.isNotEmpty(courseIdList)) {
      enrolments = userCoursesDao.listEnrolments(request.getRequestContext, userId, courseIdList);
      if (StringUtils.isNotBlank(batchId)) {
        enrolments = userCoursesDao.getEnrolmentByBatchIdAndCourseId(request.getRequestContext, userId, courseIdList.get(0), batchId);
      }
    } else {
      enrolments = userCoursesDao.listEnrolments(request.getRequestContext, userId, null);
    }
    val status: String = if (request.get(JsonKey.STATUS) != null)  request.get(JsonKey.STATUS).asInstanceOf[String] else null
    if (CollectionUtils.isNotEmpty(enrolments)) {
      enrolments = enrolments.filter(e => e.getOrDefault(JsonKey.ACTIVE, false.asInstanceOf[AnyRef]).asInstanceOf[Boolean]).toList.asJava
      if (StringUtils.isNotBlank(status)) {
        val statusValue: Integer = statusMap.getOrElse(status, -1).asInstanceOf[Integer]
        if (statusValue.intValue() != -1) {
          if (statusValue.intValue() == 1) {
            enrolments = enrolments
              .filter(e => e.getOrDefault(JsonKey.STATUS, (-1).asInstanceOf[AnyRef]).asInstanceOf[Integer] != 2)
              .toList
              .asJava
          } else {
            enrolments = enrolments
              .filter(e => e.getOrDefault(JsonKey.STATUS, (-1).asInstanceOf[AnyRef]).asInstanceOf[Integer] == statusValue)
              .toList
              .asJava
          }
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
        if (null != courseContent.get(JsonKey.DURATION)) {
          hoursSpentOnCourses = courseContent.get(JsonKey.DURATION).asInstanceOf[String].toInt
        }
        hoursSpentOnCompletedCourses += hoursSpentOnCourses
        val certificatesIssue: java.util.ArrayList[util.Map[String, AnyRef]] = courseDetails.get(JsonKey.ISSUED_CERTIFICATES).asInstanceOf[java.util.ArrayList[util.Map[String, AnyRef]]]
        if (certificatesIssue.nonEmpty) {
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
    val coursesMap = ContentCacheHandler.getContentMap.asInstanceOf[java.util.Map[String, java.util.Map[String, AnyRef]]]
    var courseContent = coursesMap.get(courseId)
    if (courseContent == null || courseContent.size() < 1)
      courseContent = ContentCacheHandler.getContent(courseId)
    courseContent
  }

  def getExternalCourseContent(courseId: String): java.util.Map[String, AnyRef] = {
    val coursesMap = ContentCacheHandler.getContentMap.asInstanceOf[java.util.Map[String, java.util.Map[String, AnyRef]]]
    var courseContent = coursesMap.get(courseId)
    if (courseContent == null || courseContent.size() < 1)
      courseContent = ContentCacheHandler.getExternalContent(courseId)
    courseContent
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
      val contentData = getContentReadAPIData(courseId, List(JsonKey.COURSECATEGORY), request)
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

  def getContentReadAPIData(programId: String, fieldList: List[String], request: Request): util.Map[String, AnyRef] = {
    val responseString: String = cacheUtil.get(programId)
    val contentData: util.Map[String, AnyRef] = if (StringUtils.isNotBlank(responseString)) {
      JsonUtil.deserialize(responseString, new util.HashMap[String, AnyRef]().getClass)
    } else {
      ContentUtil.getContentReadV3(programId, fieldList, request.getContext.getOrDefault(JsonKey.HEADER, new util.HashMap[String, String]).asInstanceOf[util.Map[String, String]])
    }
    contentData
  }

  def updateProgressData(enrolments: java.util.List[java.util.Map[String, AnyRef]], requestContext: RequestContext): util.List[java.util.Map[String, AnyRef]] = {
    enrolments.map(enrolment => {
      val leafNodesCount: Int = enrolment.getOrDefault("leafNodesCount", 0.asInstanceOf[AnyRef]).asInstanceOf[Int]
      val progress: Int = enrolment.getOrDefault("progress", 0.asInstanceOf[AnyRef]).asInstanceOf[Int]
      enrolment.put("status", getCompletionStatus(progress, leafNodesCount).asInstanceOf[AnyRef])
      enrolment.put("completionPercentage", getCompletionPerc(progress, leafNodesCount).asInstanceOf[AnyRef])
      jsonFields.foreach(field =>
        if (enrolment.containsKey(field) && null != enrolment.get(field)) {
          enrolment.put(field, mapper.readTree(enrolment.get(field).asInstanceOf[String]))
        } else enrolment.put(field, new util.HashMap[String, AnyRef]())
      )
    })
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
