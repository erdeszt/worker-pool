package com.pirum.exercises.worker

import scala.concurrent.duration.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID
import scala.concurrent.{
  Future,
  Await,
  TimeoutException,
  blocking,
  ExecutionContext
}
import scala.util.Success
import scala.util.Failure
import java.util.concurrent.atomic.AtomicReference
import zio.ZIO

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
  // NOTE: Could use a concurrent hash map instead of synchronized in onComplete
  var taskResults: Map[UUID, TaskResult] = Map.empty

  def program(
      tasks: List[Task],
      timeout: FiniteDuration,
      numberOfWorkers: Int
  ): Unit =
    val executor =
      java.util.concurrent.ScheduledThreadPoolExecutor(numberOfWorkers)
    val supervisorExecutor = Executors.newFixedThreadPool(1)

    implicit val ec = ExecutionContext.fromExecutor(supervisorExecutor)

    for (i <- 0.to(numberOfWorkers)) {
      val worker =
        Worker(this)(ExecutionContext.fromExecutor(executor))
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
        }
      }
    }

    /* NOTE: Small cheat, if we only allow `timeout` globally then
     * this will timeout sooner than the individual task timeouts so we need to account
     * for some administration overhead
     */
    Await.ready(work, timeout + 500.milliseconds)

    supervisorExecutor.shutdown()
    executor.shutdown()

    val successes = taskResults.toList
      .map {
        case (id, TaskResult.Success(duration)) => Some((id, duration))
        case _                                  => None
      }
      .collect { case Some(x) => x }

    val failures = taskResults.toList
      .map {
        case (id, TaskResult.Failure(duration)) => Some((id, duration))
        case _                                  => None
      }
      .collect { case Some(x) => x }

    val timeouts = taskResults.toList
      .map {
        case (id, TaskResult.Timeout) => Some(id)
        case _                        => None
      }
      .collect { case Some(x) => x }

    println(s"result.successful = [${successes.sortBy(_._2).mkString(",")}]")
    println(s"result.failed = [${failures.sortBy(_._2).mkString(",")}]")
    println(s"result.timedOut = [${timeouts.mkString(",")}]")

  def onComplete(taskId: UUID, result: TaskResult): Unit = {
    synchronized {
      taskResults += taskId -> result
    }
  }

}

class Worker(val supervisor: Program)(implicit val ec: ExecutionContext)
    extends Runnable {
  var taskQueue = AtomicReference(Option.empty[(UUID, Task, FiniteDuration)])
  val busy = AtomicBoolean(false)

  def execute(taskId: UUID, task: Task, timeout: FiniteDuration): Unit =
    val success = taskQueue.compareAndSet(None, Some((taskId, task, timeout)))
    assert(success)

  def readyForWork(): Boolean = {
    !busy.get() && taskQueue.get().isEmpty
  }

  def run() = {
    val isBusy = busy.getAndSet(true)
    assert(!isBusy)
    try {
      taskQueue.get() match {
        case None => ()
        case Some((taskId, task, duration)) =>
          println(s"Executing task: ${taskId}")
          val start = System.currentTimeMillis()
          try {
            Await.result(task.execute, duration)

            println(s"${taskId} Success!")

            supervisor.onComplete(
              taskId,
              TaskResult.Success(
                FiniteDuration(
                  System.currentTimeMillis - start,
                  java.util.concurrent.TimeUnit.MILLISECONDS
                )
              )
            )
          } catch {
            case e: TimeoutException =>
              println(s"${taskId} Timed out")
              supervisor.onComplete(
                taskId,
                TaskResult.Timeout
              )
            case e =>
              println(s"${taskId} Failed with: ${e}")
              supervisor.onComplete(
                taskId,
                TaskResult.Failure(
                  FiniteDuration(
                    System.currentTimeMillis - start,
                    java.util.concurrent.TimeUnit.MILLISECONDS
                  )
                )
              )
          } finally {
            taskQueue.getAndSet(None)
          }
      }
    } finally {
      busy.getAndSet(false)
    }
  }
}

class ZIOProgram extends Program {
  override def program(
      tasks: List[Task],
      timeout: FiniteDuration,
      workers: Int
  ): Unit =
    val app = ZIO
      .foreachPar(tasks) { task =>
        ZIO
          .fromFuture(_ => task.execute)
          .either
          .timed
          .disconnect
          .timeout(zio.Duration.fromScala(timeout))
          .map {
            case Some((duration, Right(_))) =>
              TaskResult.Success(
                FiniteDuration(
                  duration.toMillis(),
                  java.util.concurrent.TimeUnit.MILLISECONDS
                )
              )
            case Some((duration, Left(_))) =>
              TaskResult.Failure(
                FiniteDuration(
                  duration.toMillis(),
                  java.util.concurrent.TimeUnit.MILLISECONDS
                )
              )
            case None =>
              TaskResult.Timeout
          }
      }
      .withParallelism(workers)
      .disconnect
      .timeout(zio.Duration.fromScala(timeout + 500.millis))

    val result = zio.Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe.run(app).getOrThrowFiberFailure()
    }

    result match {
      case None => println("Failed to execute tasks in time")
      case Some(taskResults) =>
        val successes = taskResults.toList
          .map {
            case TaskResult.Success(duration) => Some(duration)
            case _                            => None
          }
          .collect { case Some(x) => x }

        val failures = taskResults.toList
          .map {
            case (TaskResult.Failure(duration)) => Some(duration)
            case _                              => None
          }
          .collect { case Some(x) => x }

        val timeouts = taskResults.toList
          .map {
            case (TaskResult.Timeout) => Some(())
            case _                    => None
          }
          .collect { case Some(x) => x }

        println(
          s"result.successful = [${successes.sorted.mkString(",")}]"
        )
        println(s"result.failed = [${failures.sorted.mkString(",")}]")
        println(s"result.timedOut = [${timeouts.mkString(",")}]")
    }

  override def onComplete(taskId: UUID, result: TaskResult): Unit = ()
}
