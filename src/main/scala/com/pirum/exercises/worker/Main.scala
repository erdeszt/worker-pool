package com.pirum.exercises.worker

import scala.concurrent.duration.*
import scala.concurrent.{Future, blocking}
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

object Main {
  val supervisorExecutor = Executors.newFixedThreadPool(4)

  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutor(supervisorExecutor)

  def test(program: Program) = {
    program.program(
      List(
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task3")
              blocking(Thread.sleep(3000))
              throw Exception("Blah")
            }
          }
        },
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task4")
              blocking(Thread.sleep(4000))
              ()
            }
          }
        },
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task2")
              blocking(Thread.sleep(2000))
              ()
            }
          }
        },
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task1")
              blocking(Thread.sleep(1000))
              throw Exception("Blah")
            }
          }
        }
      ),
      8.seconds,
      4
    )
    println("Done with main program")
  }

  def test2(program: Program) = {
    program.program(
      List(
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task3")
              blocking(Thread.sleep(3000))
              throw Exception("Blah")
            }
          }
        },
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task5")
              while (true) {
                blocking(Thread.sleep(1000))
              }
              ()
            }
          }
        },
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task4")
              blocking(Thread.sleep(4000))
              ()
            }
          }
        },
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task2")
              blocking(Thread.sleep(2000))
              ()
            }
          }
        },
        new Task {
          def execute: Future[Unit] = {
            Future {
              println("Running task1")
              blocking(Thread.sleep(1000))
              ()
            }
          }
        }
      ),
      8.seconds,
      4
    )
    println("Done with main program")
  }

  def main(args: Array[String]): Unit = {
    val test1start = System.currentTimeMillis()
    test(ZIOProgram())
    println(s"Test 1 took: ${System.currentTimeMillis - test1start}")
    val test2start = System.currentTimeMillis()
    test2(ZIOProgram())
    println(s"Test 2 took: ${System.currentTimeMillis - test2start}")
    supervisorExecutor.shutdownNow()
  }

}
