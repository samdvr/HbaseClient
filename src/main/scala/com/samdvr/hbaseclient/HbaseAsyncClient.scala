package com.samdvr.hbaseclient

import cats.effect.ConcurrentEffect
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.filter.{Filter, PrefixFilter}
import fs2._
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait HbaseAsyncClient[F[_]] {

  def get(table: Array[Byte], row: Array[Byte], column: Array[Byte], qualifier: Array[Byte]): F[Result]

  def get(table: Array[Byte], row: Array[Byte]): F[Result]

  def put(table: Array[Byte], row: Array[Byte]): F[Result]

  def scan(table: Array[Byte], filter: Option[Filter] = None): Stream[F, Result]

  def scanWithPrefix(prefix: Array[Byte], table: Array[Byte]): Stream[F, Result]
}

object HbaseAsyncClient {
  def apply[F[_]](connection: F[AsyncConnection])(
    implicit ec: ExecutionContext, F: ConcurrentEffect[F]): HbaseAsyncClient[F] = new HbaseAsyncClient[F] {

    private def connectionBracket: Stream[F, AsyncConnection] = {
      Stream.bracket(connection)(c => F.delay(c.close()))
    }

    override def get(table: Array[Byte], row: Array[Byte]): F[Result] = {
      F.async { cb =>
        connectionBracket
          .map(c => c.getTable(TableName.valueOf(table)))
          .map(x => FutureConverters.toScala(x.get(
            new Get(row)).toCompletableFuture).onComplete {
            case Success(value) => cb(Right(value))
            case Failure(exception) => cb(Left(exception))
          })

      }
    }

    override def get(table: Array[Byte],
                     row: Array[Byte],
                     column: Array[Byte],
                     qualifier: Array[Byte]): F[Result] = F.async { cb =>
      connectionBracket
        .map(c => c.getTable(TableName.valueOf(table)))
        .map(x => FutureConverters.toScala(x.get(
          new Get(row).addColumn(column, qualifier)).toCompletableFuture).onComplete {
          case Success(value) => cb(Right(value))
          case Failure(exception) => cb(Left(exception))
        })

    }

    override def scan(table: Array[Byte], filter: Option[Filter] = None): Stream[F, Result] = {
      filter match {
        case Some(fa) =>
          for {
            conn <- connectionBracket
            result <- fs2.Stream.emits(conn.getTable(TableName.valueOf(table))
              .getScanner(new Scan()
                .setFilter(fa)).asScala.toList).covary[F]
          } yield result
        case None =>
          for {
            conn <- Stream.eval(connection)
            result <- fs2.Stream.emits(conn.getTable(TableName.valueOf(table))
              .getScanner(new Scan())
              .asScala.toList).covary[F]
          } yield result

      }

    }

    override def scanWithPrefix(prefix: Array[Byte], table: Array[Byte]): Stream[F, Result] = {
      for {
        conn <- connectionBracket
        result <- fs2.Stream.emits(
          conn.getTable(TableName.valueOf(table))
            .getScanner(new Scan()
              .setFilter(new PrefixFilter(prefix))).asScala.toList).covary[F]
      } yield result

    }

    override def put(table: Array[Byte], row: Array[Byte]): F[Result] = F.async { cb =>
      connectionBracket
        .map(_.getTable(TableName.valueOf(table))
          .put(new Put(row)).toCompletableFuture)
    }
  }


}