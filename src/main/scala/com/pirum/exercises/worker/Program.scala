package com.pirum.exercises.worker

import scala.concurrent.duration.FiniteDuration
import java.util.UUID
import scala.concurrent.{Future, Await, TimeoutException, blocking}
// import scala.concurrent.ExecutionContext.Implicits.global

trait Program {
  def program(tasks: List[Task], timeout: FiniteDuration, workers: Int): Unit
  def onComplete(taskId: UUID, result: TaskResult): Unit
}

enum TaskResult {
  case Success(duration: FiniteDuration)
  case Failure(duration: FiniteDuration)
  case Timeout
}

class LiveProgram extends Program {
  var workers: List[Worker] = List.empty
  var taskResults: Map[UUID, TaskResult] = Map.empty

  def program(tasks: List[Task], timeout: FiniteDuration, numberOfWorkers: Int): Unit =
    val executor = java.util.concurrent.ScheduledThreadPoolExecutor(numberOfWorkers)
    val supervisorExecutor = ThreadPoolExecutor(1)

    implicit val ec = supervisorExecutor

    for (i <- 0.to(numberOfWorkers)) {
      val worker = Worker(this)
      workers = worker :: workers

      executor.scheduleAtFixedRate(
        worker,
        0,
        10,
        java.util.concurrent.TimeUnit.MILLISECONDS
      )
    }

    var working = true
    var remainingTasks = tasks

    val work = Future {
      while (working) {
        remainingTasks match {
          case Nil => working = false
          case task :: remaining =>
            var submitted = false
            for (worker <- workers) {
              if (!submitted && worker.readyForWork()) {
                val id = UUID.randomUUID()
                worker.execute(id, task, timeout)
                remainingTasks = remaining
                submitted = true
              }
            }
        }
      }

      println("All tasks submitted")

      var moreWorkToDo = true

      blocking {
        while (moreWorkToDo) {
          var allDone = true


          for (worker <- workers) {
            if (!worker.readyForWork()) {
              allDone = false
            }
          }

          if (allDone) {
            moreWorkToDo = false
          }

          // Thread.`yield`()
        }
      }
    }

    Await.ready(work, timeout)

    val successes = taskResults.toList.map {
      case (id, TaskResult.Success(duration))  => Some((id, duration))
      case _ => None
    }.collect { case Some(x) => x}

    val failures = taskResults.toList.map {
      case (id, TaskResult.Failure(duration))  => Some((id, duration))
      case _ => None
    }.collect { case Some(x) => x}

    val timeouts = taskResults.toList.map {
      case (id, TaskResult.Timeout)  => Some(id)
      case _ => None
    }.collect { case Some(x) => x}

    println(s"result.successful = [${successes.sortBy(_._2).mkString(",")}]")
    println(s"result.failed = [${failures.sortBy(_._2).mkString(",")}]")
    println(s"result.timedOut = [${timeouts.mkString(",")}]")

    
  def onComplete(taskId: UUID, result: TaskResult) : Unit = {
    taskResults += taskId -> result
  }


}

class Worker(val supervisor: Program) extends Runnable {
  // TODO: Might need to indicate if we are ready to receive a task
  var taskQueue = Option.empty[(UUID, Task, FiniteDuration)]

  def execute(taskId: UUID, task: Task, timeout: FiniteDuration): Unit =
    assert(taskQueue.isEmpty == true)
    taskQueue = Some((taskId, task, timeout))

  def readyForWork(): Boolean = {
    taskQueue.isEmpty
  }

  def run() = {
    // TODO: Synchronize
    taskQueue match {
      case None => ()
      case Some((taskId, task, duration)) => 
        println(s"Executing task: ${taskId}")
        val start = System.currentTimeMillis()
        try {
          Await.ready(task.execute, duration)
          supervisor.onComplete(
            taskId, 
            TaskResult.Success(
              FiniteDuration(System.currentTimeMillis - start, java.util.concurrent.TimeUnit.MILLISECONDS)
            )
          )
        } catch {
          case e: InterruptedException =>
          supervisor.onComplete(
            taskId, 
            TaskResult.Failure(
              FiniteDuration(System.currentTimeMillis - start, java.util.concurrent.TimeUnit.MILLISECONDS)
            )
          )
          case e: TimeoutException =>
            supervisor.onComplete(taskId, TaskResult.Timeout)
          case e => throw e
        } finally {
          println(s"Done executing ${taskId}")
          taskQueue = None
        }

    }
  }
}
