package org.openworm.trackercommons

import fastparse.all._

object Parser {
  object Text {
    val White = P( CharsWhile(_.isWhitespace) )
    def W[A](p: P[A]): P[A] = White.? ~ p ~ White.?
    val Null = P("null")
    val Bool = P("true" | "false")
    val Hex = P(CharIn("0123456789ABCDEFabcdef"))
    val OneToNine = P( CharIn("123456789") )
    val Digit = P( CharIn("0123456789") )
    val Digits = P( CharsWhile(c => c >= '0' && c <= '9') )
    val Str = P(
      "\"" ~
        ( CharsWhile(c => c != '"' && c != '\\') |
          ("\\" ~ (("u" ~ Hex ~ Hex ~ Hex ~ Hex) | CharIn("\"\\/bfnrt"))
          )
        ).rep ~
      "\""
    )
    val Num = P(
      "-".? ~
      ("0" ~! Pass | Digits) ~
      ("." ~ Digits).? ~
      (IgnoreCase("e").? ~ CharIn("+-").? ~ Digits).?
    )
    val KeyVal = P( Str ~ W(":") ~! Val )
    val Arr = P( "[" ~ W(Val.rep(sep = W(","))) ~ "]" )
    val Obj = P( "{" ~ W(KeyVal.rep(sep = W(","))) ~ "}" )
    val Val: P[Unit] = P( Obj | Arr | Str | Num | Bool | Null )
    val All = W(Val.!)
  }

  object Obj {
    import Text.{Digit => D}
    val Date = P(
      (D ~ D ~ D ~ D).! ~ "-" ~ (D ~ D).! ~ "-" ~ (D ~ D).! ~ "T" ~                   // Date
      (D ~ D).! ~         ":" ~ (D ~ D).! ~ ":" ~ (D ~ D ~ ("." ~ D.rep(1)).?).! ~    // Time
      ("Z" | (CharIn("+-") ~ D ~ D ~ ":" ~ D ~ D)).!.?                                // Locale
    ).map{ case (y,mo,d,h,mi,s,loc) => 
      val ss = s.toDouble
      val ssi = math.floor(ss).toInt
      val ssns = math.rint((ss - ssi)*1e9).toInt
      (java.time.LocalDateTime.of(y.toInt, mo.toInt, d.toInt, h.toInt, mi.toInt, ssi, ssns), loc.getOrElse(""))
    }

    val Age = P((D rep 1).! ~ ":" ~ (D ~ D).! ~ ":" ~ (D ~ D ~ ("." ~ D.rep(1)).?).!).map{ case (h,m,s) => 
      val ss = s.toDouble
      val ssi = math.floor(ss).toInt
      val ssns = math.rint((ss - ssi)*1e9).toInt
      java.time.Duration.ZERO.withSeconds(h.toLong*3600L + m.toInt*60 + ssi).withNanos(ssns)
    }

    val Real = (
      Text.Num.!.map(_.toDouble) |
      Text.Null.map(_ => Double.NaN) |
      IgnoreCase("\"nan\"").map(_ => Double.NaN) |  // Not recommended, but valid JSON
      IgnoreCase("nan").!.map(_ => Double.NaN) |    // Not even valid JSON, don't do this
      (IgnoreCase("\"inf") ~ IgnoreCase("inity").? ~ "\"").map(_ => Double.PositiveInfinity) |   // Not recommended, but valid JSON
      (IgnoreCase("inf") ~ IgnoreCase("inity").?).map(_ => Double.PositiveInfinity) |            // Not even valid JSON, don't do this
      (IgnoreCase("\"-inf") ~ IgnoreCase("inity").? ~ "\"").map(_ => Double.NegativeInfinity) |  // Not recommended, but valid JSON
      (IgnoreCase("-inf") ~ IgnoreCase("inity").?).map(_ => Double.NegativeInfinity)             // Not even valid JSON, don't do this
    )

    val Realz = "[" ~ Text.W(Real).rep(sep = Text.W(",")).map(_.toArray) ~ "]"

    val Reals = Real.map(x => Array(x)) | Realz

    val Realzz = "[" ~ Text.W(Reals).rep(sep = Text.W(",")).map(_.toArray) ~ "]"

    val Realss = Realz.map(x => Array(x)) | Realzz
  }

  val ofUnits = {
    val UnitKV = Text.Str.! ~ Text.W(":") ~ Text.Str.! ~! Pass
    val u = new Units
    P( "{" ~ Text.W(UnitKV.filter{ case (k,v) => u += (k, v) }).rep(3, sep = Text.W(",")) ~ "}" ).map(_ => u)
  }

  def apply(s: String) = Text.All.parse(s)
}