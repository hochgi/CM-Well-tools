import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class MassiveGraphReplaceSpec
  extends Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  override def afterAll: Unit = {
//    TestKit.shutdownActorSystem(system)
  }

  // #test_snippet
  "An HelloAkkaActor" should "be able to set a new greeting" in {
//    val greeter = TestActorRef(Props[Greeter])
//    greeter ! WhoToGreet("testkit")
//    greeter.underlyingActor.asInstanceOf[Greeter].greeting should be("hello, testkit")
  }

  it should "be able to get a new greeting" in {
//    val greeter = system.actorOf(Props[Greeter], "greeter")
//    greeter ! WhoToGreet("testkit")
//    greeter ! Greet
//    expectMsgType[Greeting].message.toString should be("hello, testkit")
  }
}
