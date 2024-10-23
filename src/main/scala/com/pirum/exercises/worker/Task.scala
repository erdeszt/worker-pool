package com.pirum.exercises.worker

import scala.concurrent.Future

// A task that either succeeds after n seconds, fails after n seconds, or never terminates
trait Task {
  def execute: Future[Unit]
}

