package com.weather.streamsx.cassandra.connection

import java.util.concurrent.{TimeUnit, TimeoutException}

import com.datastax.driver.core.exceptions.{UnauthorizedException, NoHostAvailableException, UnavailableException}
import com.datastax.driver.core.{ResultSet, ResultSetFuture}
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import com.weather.streamsx.util.{StringifyStackTrace => SST}

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


  // TODO: add for authentication errors
  def getTypicalExceptionHandler: PartialFunction[Any, AwaitResultSetV] = {
    case ex: NoHostAvailableException if ex.getErrors.size() == 0 =>
      log.error(s"Handled NoHostAvailableException with 0 errors. Pausing....$ex\n${SST(ex)}")
      Failure( CassandraWriterException(s"Handled NoHostAvailableException with 0 errors. Pausing....", ex) )
    case ex: UnavailableException =>
      log.error(s"Handled UnavailableException. Pausing...$ex\\n${SST(ex)}")
      Failure( CassandraWriterException(s"Handled UnavailableException. Pausing...", ex) )
    case ex: TimeoutException =>
      log.error(s"Handled TimeoutException. Pausing..$ex\n${SST(ex)}")
      Failure( CassandraWriterException(s"Handled TimeoutException. Pausing...", ex) )
    case ex: UnauthorizedException =>
      log.error(s"Encountered UnauthorizedException. $ex\n${SST(ex)}")
      throw new CassandraWriterException("Streams application not authorized to modify table. Please check your authentication settings", ex)
    case e: Throwable =>
      log.error(s"Error awaiting futures$e\n${SST(e)}")
      Failure( CassandraWriterException(s"Error awaiting futures", e) )
  }

  protected def logFailure(v: AwaitResultSetV) = v match {
    case Failure(f) => log.error("Cassandra write failed", f)
    case _ => ()
  }
}