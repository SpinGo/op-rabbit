package com.spingo.op_rabbit

import play.api.libs.json._

trait PgChangeSetLike[T] {
  val `new`: Option[T]
  val `old`: Option[T]
}

trait PgChangeLike[T] {
  val table: String
  val key: Option[String]
  val op: String
  val data: PgChangeSetLike[T]

  def map[R](delete: Function1[T, Option[R]] = PgChangeMap.noop1,
             insert: Function1[T, Option[R]] = PgChangeMap.noop1,
             update: Function2[T, T, Option[R]] = PgChangeMap.noop2) = this match {
    case DeleteOp(_, _, old) => delete(old)
    case InsertOp(_, _, n) => insert(n)
    case UpdateOp(_, _, old, n) => update(old, n)
  }
}

case class PgTypedChangeSet[T](`new`: Option[T], `old`: Option[T]) extends PgChangeSetLike[T]
case class PgTypedChange[T](table: String, key: Option[String], op: String, data: PgChangeSetLike[T]) extends PgChangeLike[T]
case class PgChangeSet(`new`: Option[JsObject], `old`: Option[JsObject]) extends PgChangeSetLike[JsObject]
case class PgChange(table: String, key: Option[String], op: String, data: PgChangeSet) extends PgChangeLike[JsObject] {
  def as[T](implicit reader: Reads[T]): PgTypedChange[T] = {
    PgTypedChange(table, key, op,
      PgTypedChangeSet(`new` = data.`new`.map(reader.reads(_).get), `old` = data.old.map(reader.reads(_).get)))
  }

}

object PgChangeJsonFormats {
  implicit val pgChangeSetFormat = Json.format[PgChangeSet]
  implicit val pgChangeFormat = Json.format[PgChange]
}

object DeleteOp {
  def unapply[T](change: PgChangeLike[T]): Option[(String, Option[String], T)] = {
    if(change.op == "DELETE")
      Some((change.table, change.key, change.data.old.get))
    else
      None
  }
}
object InsertOp {
  def unapply[T](change: PgChangeLike[T]): Option[(String, Option[String], T)] = {
    if (change.op == "INSERT")
      Some(change.table, change.key, change.data.`new`.get)
    else
      None
  }
}

object UpdateOp {
  def unapply[T](change: PgChangeLike[T]): Option[(String, Option[String], T, T)] = {
    if (change.op == "UPDATE")
      Some(change.table, change.key, change.data.`old`.get, change.data.`new`.get)
    else
      None
  }
}

object InsertOrUpdateOp {
  def unapply[T](change: com.spingo.op_rabbit.PgChangeLike[T]): Option[(String, Option[String], T)] = {
    if (change.op == "INSERT" || change.op == "UPDATE")
      Some(change.table, change.key, change.data.`new`.get)
    else
      None
  }
}

object PgChangeMap {
  val noop1 = { a: Any => None }
  val noop2 = { (a: Any, b: Any) => None }
}

object Pg {
  case class Table(table:String) {
    def all = s"postgres.${table}.#"
    def update = s"postgres.${table}.UPDATE"
    def delete = s"postgres.${table}.DELETE"
    def insert = s"postgres.${table}.INSERT"
  }
}
