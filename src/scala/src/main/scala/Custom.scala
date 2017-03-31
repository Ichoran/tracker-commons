package org.openworm.trackercommons

import scala.collection.mutable.{ AnyRefMap, ListBuffer }

import kse.jsonal._
import kse.jsonal.JsonConverters._

import WconImplicits._

trait Customizable[A] { self: A =>
  def custom: Json.Obj
  def customFn(f: Json.Obj => Json.Obj): A
}

object Custom {
  def apply(o: Json.Obj) = o.filter{ case (k, _) => k startsWith "@" }

  case class Lossy[A](best: A, lost: Int) {
    def isDisaster = lost == Int.MinValue
    def isMissing = lost == Int.MaxValue || isDisaster
    def isIncomplete = lost != 0
    def map[B](f: A => B): Lossy[B] = new Lossy(f(best), lost)
    def flatMap[B](f: A => Lossy[B]): Lossy[B] = {
      val that = f(best)
      val worst =
        if (lost == Int.MinValue || that.lost == Int.MinValue) Int.MinValue
        else if (lost == Int.MaxValue || that.lost == Int.MaxValue) Int.MaxValue
        else if (lost < 0 || that.lost < 0) math.min(lost, that.lost)
        else math.max(lost, that.lost)
      new Lossy(that.best, worst)
    }
  }
  object Lossy {
    def isDisaster(lost: Int) = lost == Int.MinValue
    def isMissing(lost: Int) = lost == Int.MaxValue || lost == Int.MinValue
    def disaster[A](best: A) = new Lossy(best, Int.MinValue)
    def missing[A](best: A) = new Lossy(best, Int.MaxValue)
    def perfect[A](best: A) = new Lossy(best, 0)
    def summarizing[A](best: A, losses: Array[Int]) = new Lossy(best, summarizeLosses(losses))
    def summarizeLosses(losses: Array[Int]): Int = {
      var i = 0
      var corrupted = 0
      var totaled = 0
      var obliterated = false
      while (!obliterated && i < losses.length) {
        val li = losses(i)
        if (li == Int.MinValue) obliterated = true
        else if (li == Int.MaxValue) totaled += 1
        else if (li != 0) corrupted += 1
        i += 1
      }
      if (obliterated) Int.MinValue
      else if (totaled == i) Int.MaxValue
      else if (totaled > 0) -totaled
      else corrupted
    }
  }

  abstract class Strategy[A] {
    def split(context: A, reshape: Reshape, o: Json.Obj): Lossy[Json.Obj]
    def merge(contexts: Array[A], join: Join, os: Array[Json.Obj]): Lossy[Json.Obj]
  }

  abstract class KeyedStrategy[A](val counter: A => Int) extends Strategy[A] {
    def keySplit(context: A, key: String, reshape: Reshape, j: Json): Lossy[Json]
    def subkeySplit(context: A, key: String, subkey: String, reshape: Reshape, j: Json): Lossy[Json]

    def keyMerge[J <: Json](contexts: Array[A], key: String, join: Join, js: Array[J]): Lossy[Json]
    def subkeyMerge[J <: Json](contexts: Array[A], key: String, subkey: String, join: Join, js: Array[J]): Lossy[Json]

    private def mySplit(context: A, reshape: Reshape, o: Json.Obj, key: Option[String]): Lossy[Json.Obj] = {
      val ks = new Array[String](o.size)
      val vs = new Array[Json](o.size)
      val ls = new Array[Int](o.size)
      var i = 0
      val oi = o.iterator
      while (oi.hasNext) {
        val (k, v) = oi.next
        ks(i) = k
        v match {
          case jo: Json.Obj if key.isEmpty =>
            val x = mySplit(context, reshape, jo, Some(k))
            vs(i) = x.best
            ls(i) = x.lost
          case ja: Json.Arr if ja.size == counter(context) =>
            vs(i) = reshape(ja)
            ls(i) = 0
          case _ =>
            val x = key match {
              case Some(q) => subkeySplit(context, q, k, reshape, v);
              case None    => keySplit(context, k, reshape, v);
            }
            vs(i) = x.best
            ls(i) = x.lost
        }
        i += 1
      }
      val lost = Lossy.summarizeLosses(ls)
      if (lost == 0) Lossy.perfect(if (o.isArrayBacked) Json.Obj(ks, vs).asInstanceOf[Json.Obj] else Json.Obj((ks, vs).zipped.toMap))
      else if (lost == Int.MinValue) Lossy.disaster(Json.Obj.empty)
      else if (lost == Int.MaxValue) Lossy.missing(Json.Obj.empty)
      else if (o.isArrayBacked) {
        if (lost > 0) new Lossy(Json.Obj(ks, vs).asInstanceOf[Json.Obj], lost)
        else {
          val kvs = new Array[AnyRef]((ls.length + lost)*2)
          i = 0
          var j = 0
          while (i < ls.length) {
            if (ls(i) != Int.MaxValue) {
              kvs(j) = ks(i)
              kvs(j+1) = vs(i)
              j += 2
            }
            i += 1
          }
          new Lossy(Json.Obj.fromFlatArray(kvs), lost)
        }
      }
      else {
        val m = new AnyRefMap[String, Json]
        i = 0
        while (i < ls.length) {
          if (ls(i) != Int.MaxValue) m += (ks(i), vs(i))
          i += 1
        }
        new Lossy(Json.Obj(m), lost)
      }
    }
    final def split(context: A, reshape: Reshape, o: Json.Obj): Lossy[Json.Obj] = mySplit(context, reshape, o, None)

    private def allKeys(os: Array[Json.Obj]): AnyRefMap[String, String] = {
      val m = new AnyRefMap[String, String]
      os.foreach{_.iterator.foreach{ case (k, _) => m += (k, k) }}
      m
    }
    private def normalized(required: AnyRefMap[String, String], o: Json.Obj): AnyRefMap[String, Json] = {
      val m = new AnyRefMap[String, ListBuffer[Json]]
      o.iterator.foreach{ case (k, v) => m.getOrElseUpdate(k, new ListBuffer[Json]) += v }
      required.mapValuesNow{ k =>
        m.get(k) match {
          case None => Json.Null
          case Some(lb) =>
            if (lb.size == 1) lb.head
            else Json(lb)
        }
      }
    }
    private def exploded(os: Array[Json.Obj]): AnyRefMap[String, Array[Json]] = {
      val keys = allKeys(os)
      val normed = os.map(o => normalized(keys, o))
      keys.mapValuesNow{ k => normed.map(ni => ni(k)) }
    }
    private def arrayablySized(contexts: Array[A], actual: Array[Json]): Option[Array[Json.Arr]] = {
      var i = 0
      while (i < actual.length) {
        actual(i) match {
          case ja: Json.Arr if ja.size == counter(contexts(i)) =>
          case _ => return None
        }
        i += 1
      }
      Some(actual.map(_.asInstanceOf[Json.Arr]))
    }
    private def objectlySized(contexts: Array[A], actual: Array[Json]): Option[Array[Json.Obj]] = {
      var i = 0
      while (i < actual.length) {
        actual(i) match {
          case o: Json.Obj =>
          case _ => return None
        }
        i += 1
      }
      Some(actual.map(_.asInstanceOf[Json.Obj]))
    }
    def merge(contexts: Array[A], join: Join, os: Array[Json.Obj]): Lossy[Json.Obj] = {
      var disaster = false
      val everything = exploded(os)
      val arrays = new AnyRefMap[String, Json.Arr]
      everything.foreach{ case (k, js) => 
        arrayablySized(contexts, js).map(as => arrays += (k, join(as)))
      }
      val objects = new AnyRefMap[String, Lossy[Json.Obj]]
      everything.foreach{ case (k, js) => if (!arrays.contains(k)) {
        objectlySized(contexts, js) match {
          case Some(oz) =>
            val subevery = exploded(oz)
            val subarrays = new AnyRefMap[String, Json.Arr]
            subevery.foreach{ case (kk, js) =>
              arrayablySized(contexts, js).map(as => subarrays += (kk, join(as)))
            }
            val subremains = new AnyRefMap[String, Lossy[Json]]
            subevery.foreach{ case (kk, js) => if (!subarrays.contains(kk)) {
              subremains += (kk, subkeyMerge(contexts, k, kk, join, js))  
            }}
            val loss = Lossy.summarizeLosses(subremains.map(_._2.lost).toArray)
            if (Lossy.isDisaster(loss)) disaster = true
            val m = new AnyRefMap[String, Json]
            subarrays.foreach{ case (kk, ar) => m += (kk, ar) }
            if (!Lossy.isMissing(loss)) subremains.foreach{ case (kk, ljo) => if (!ljo.isMissing) m += (kk, ljo.best) }
            if (m.nonEmpty) objects += (k, Lossy(Json.Obj(m), if (Lossy.isMissing(loss)) -subremains.size else loss))
          case None =>
        }
      }}
      val remainder = new AnyRefMap[String, Lossy[Json]]
      everything.foreach{ case (k, js) =>
        if (!arrays.contains(k) && !objects.contains(k)) remainder += (k, keyMerge(contexts, k, join, js))
      }
      val m = new AnyRefMap[String, Json]
      val losses: Array[Int] = Array(arrays.map(_ => 0).toArray, objects.map(_._2.lost).toArray, remainder.map(_._2.lost).toArray).reduce(_ ++ _)
      val loss = Lossy.summarizeLosses(losses)
      arrays.foreach{ case (k, ar) => m += (k, ar) }
      objects.foreach{ case (k, ob) => m += (k, ob.best) }
      remainder.foreach{ case (k, rm) => m += (k, rm.best) }
      Lossy(Json.Obj(m), loss)
    }
  }

  abstract class SizedStrategy[A](counter: A => Int) extends KeyedStrategy[A](counter) {
    def oneSplit(count: Int, reshape: Reshape, j: Json): Lossy[Json]
    def oneMerge[J <: Json](counts: Array[Int], join: Join, js: Array[J], deeper: Boolean): Lossy[Json]

    final def keySplit(context: A, tag: String, reshape: Reshape, j: Json) =
      oneSplit(counter(context), reshape, j)
    final def subkeySplit(context: A, tag: String, subtag: String, reshape: Reshape, j: Json) =
      oneSplit(counter(context), reshape, j)
    final def keyMerge[J <: Json](contexts: Array[A], key: String, join: Join, js: Array[J]): Lossy[Json] =
      oneMerge(contexts.map(counter), join, js, true)    
    final def subkeyMerge[J <: Json](contexts: Array[A], key: String, subkey: String, join: Join, js: Array[J]): Lossy[Json] =
      oneMerge(contexts.map(counter), join, js, true)
  }
  class Discard[A](counter: A => Int) extends SizedStrategy(counter) {
    def oneSplit(count: Int, reshape: Reshape, j: Json): Lossy[Json] = Lossy.missing(Json.Null)
    def oneMerge[J <: Json](counts: Array[Int], join: Join, js: Array[J], deeper: Boolean): Lossy[Json] = Lossy.missing(Json.Null)
  }
  class Expand[A](counter: A => Int) extends SizedStrategy(counter) {
    def oneSplit(count: Int, reshape: Reshape, j: Json): Lossy[Json] = j match {
      case jn: Json.Num if jn.isDouble => Lossy.perfect(Json.Arr.Dbl(reshape(Array.fill(count)(jn.double))))
      case ja: Json.Arr                => Lossy.missing(Json.Null)
      case _                           => Lossy.perfect(Json.Arr.All(reshape(Array.fill(count)(j))))
    }
    def oneMerge[J <: Json](counts: Array[Int], join: Join, js: Array[J], deeper: Boolean): Lossy[Json] = {
      if (js.exists(_.isInstanceOf[Json.Arr]) || js.isEmpty) Lossy.missing(Json.Null)
      else if ((js.iterator zip js.iterator.drop(1)).forall{ case (jl, jr) => jl == jr }) Lossy.perfect(js.head)
      else {
        val a = new Array[Json.Arr](counts.length)
        var i = 0
        var fine = true
        while (fine && i < a.length) {
          a(i) = js(i) match {
            case jn: Json.Num if jn.isDouble => Json.Arr.Dbl(Array.fill(counts(i))(jn.double))
            case ja: Json.Arr                => fine = false; Json.Arr.All.empty
            case jo: Json.Obj if !deeper && jo.iterator.exists(_._2 match { case _: Json.Arr => true; case _ => false }) => fine = false; Json.Arr.All.empty
            case j                           => Json.Arr.All(Array.fill(counts(i))(j))
          }
          i += 1
        }
        if (fine) Lossy.perfect(join(a))
        else Lossy.missing(Json.Null)
      }
    }
  }
}
