package com.pirum.exercises.worker

import scala.concurrent.duration.*
import scala.concurrent.{Future, blocking}
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  // def program(tasks: List[Task], timeout: FiniteDuration, workers: Int): Unit =
  //   ???
  //

  val program = LiveProgram()

  program.program(
    List(
      new Task {
        def execute: Future[Unit] = {
          Future {
            println("Running task1")
            blocking(Thread.sleep(3))
            throw Exception("Blah")
          }
        }
      },
      new Task {
        def execute: Future[Unit] = {
          Future {
            println("Running task2")
            blocking(Thread.sleep(4))
            ()
          }
        }
      },
      new Task {
        def execute: Future[Unit] = {
          Future {
            println("Running task3")
            blocking(Thread.sleep(2))
            ()
          }
        }
      },
      new Task {
        def execute: Future[Unit] = {
          Future {
            println("Running task4")
            blocking(Thread.sleep(1))
            throw Exception("Blah")
          }
        }
      }
    ),
    8.seconds,
    4
  )

}
