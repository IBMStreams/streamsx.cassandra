package com.weather.streamsx.cassandra

import java.util.concurrent.{TimeUnit, TimeoutException}

import com.datastax.driver.core.exceptions.{NoHostAvailableException, UnavailableException}
import com.datastax.driver.core.{ResultSet, ResultSetFuture}

import scalaz._

trait CassandraAwaiter {
  protected val log: org.slf4j.Logger
  protected val writeOperationTimeout: Long
  type AwaitResultSetV = Validation[Exception, ResultSet]

  private def getTimeLeft(end: Long) = {
    val r = end - System.currentTimeMillis
    if (r < 0L) 0L else r
  }

  def await(timeout: Long = writeOperationTimeout)(futures: Seq[ResultSetFuture]): Seq[AwaitResultSetV] = {
    val end = System.currentTimeMillis + timeout
    futures map { awaitOne(getTimeLeft(end)) }
  }

  def awaitOne(
    timeout: Long = writeOperationTimeout,
    exceptionHandler: PartialFunction[Any, AwaitResultSetV] = getTypicalExceptionHandler)(
    future: ResultSetFuture): AwaitResultSetV =
    try Success(  future.getUninterruptibly(timeout, TimeUnit.MILLISECONDS) ) catch exceptionHandler

  def getTypicalExceptionHandler: PartialFunction[Any, AwaitResultSetV] = {
    case ex: NoHostAvailableException if ex.getErrors.size() == 0 =>
      log.error("Handled NoHostAvailableException with 0 errors. Pausing....{}", ex)
      Failure( CassandraWriterException(s"Handled NoHostAvailableException with 0 errors. Pausing....", ex) )
    case ex: UnavailableException =>
      log.error("Handled UnavailableException. Pausing...{}", ex)
      Failure( CassandraWriterException(s"Handled UnavailableException. Pausing...", ex) )
    case ex: TimeoutException =>
      log.error("Handled TimeoutException. Pausing...{}", ex)
      Failure( CassandraWriterException(s"Handled TimeoutException. Pausing...", ex) )
    case e: Throwable =>
      log.error(s"Error awaiting futures{}", e)
      Failure( CassandraWriterException(s"Error awaiting futures", e) )
  }

  protected def logFailure(v: AwaitResultSetV) = v match {
    case Failure(f) => log.error("Cassandra write failed", f)
    case _ => ()
  }
}