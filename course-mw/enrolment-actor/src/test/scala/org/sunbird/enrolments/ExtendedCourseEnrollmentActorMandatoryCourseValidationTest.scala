package org.sunbird.enrolments

import java.util.concurrent.TimeUnit
import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.cache.util.RedisCacheUtil
import org.sunbird.common.models.response.Response
import org.sunbird.common.request.Request

import scala.concurrent.duration.FiniteDuration

/**
 * Covers the one branch of ExtendedCourseEnrollmentActor#validateMandatoryCourseCompletion that
 * is unit-testable with this repo's Scala tooling (ScalaMock cannot intercept static Java calls
 * or singletons). The remaining branches - plan-not-found (notEligibleForAssessment),
 * mandatory-course access-settings ineligibility (mandatoryCoursesAccessRestricted), and
 * mandatory-course incompletion (mandatoryCoursesNotCompleted) - all depend on the static
 * CbPlanUtil.getCbPlanDictionary (real HTTP call) and AccessSettingsUtil (calls
 * AccessSettingsDaoImpl.getInstance()/UserOrgServiceImpl.getInstance()), which are covered
 * instead at the unit level in course-actors-common's JUnit+PowerMock suite:
 * AccessSettingsUtilTest, RuleEngineValidatorTest, CbPlanUtilTest.
 */
class ExtendedCourseEnrollmentActorMandatoryCourseValidationTest extends FlatSpec with Matchers with MockFactory {
  val system = ActorSystem.create("system")
  val cacheUtil = mock[RedisCacheUtil]

  "validateMandatoryCourseCompletion" should "no-op with success for a non-Comprehensive-Assessment do_id" in {
    (cacheUtil.get(_: String, _: String => String, _: Int)).expects("do_course_1", *, *).returns("{\"courseCategory\":\"Course\"}")
    val response = callActor(getValidateRequest("do_course_1"), Props(new ExtendedCourseEnrollmentActor(null)(cacheUtil)))
    assert("Success".equalsIgnoreCase(response.get("response").asInstanceOf[String]))
  }

  def getValidateRequest(doId: String): Request = {
    val request = new Request
    request.setOperation("validateMandatoryCourseCompletion")
    request.put("userId", "user1")
    request.put("courseId", doId)
    request
  }

  def callActor(request: Request, props: Props): Response = {
    val probe = new TestKit(system)
    val actorRef = system.actorOf(props)
    actorRef.tell(request, probe.testActor)
    probe.expectMsgType[Response](FiniteDuration.apply(10, TimeUnit.SECONDS))
  }
}
