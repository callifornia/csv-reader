package com.hometask
import scala.annotation.tailrec
import scala.concurrent.duration._

object Task1 {

  /* example
  *  retry[Int](
  *   block        =  () => 1 + 1,
  *   acceptResult = res => res % 2 == 0
  *   retries      = List(0.seconds, 1.seconds, 2.seconds)
  * */
  @tailrec
  def retry[A](block: () => A,
               acceptResult: A => Boolean,
               retries: List[FiniteDuration]): A = {

//    "1.71.0-SNAPSHOT"
//    1.71.0-SNAPSHOT
//    1.71.0-SNAPSHOT
//    1.71.0-SNAPSHOT
//    1.71.0-SNAPSHOT

    (retries, block()) match {
      case (Nil, result) => result
      case (_, result) if acceptResult(result) => result
      case (list, _) =>
        Thread.sleep(list.head.toMillis)
        retry(block, acceptResult, list.tail)
    }
  }
}
