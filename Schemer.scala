package schemer

import play.api.libs.json._

import scala.io.Source

class Schemer(file: String = "") {
	var schema: JsValue = Json.obj()
	var lines = 0

	if (!file.isEmpty) loadFile(file)

	def addLine(line: String) {
		lines += 1
		val json = Json.parse(line)
		schema = merge(schema, json)
	}

	def loadFile(name: String) {
		for (line <- Source.fromFile(name).getLines)
			addLine(line)
	}



	case class RowMismatch(a: JsValue, b: JsValue) extends Throwable {
		override def toString = Seq(
			s"On the line $lines you attempted to insert this JSON:",
			Json.prettyPrint(b),
			"with the corresponding schema:",
			out(b),
			"into the schema with this signature:",
			out(a)
		) mkString "\n"
	}

	case class InconsistentArray(arr: Seq[JsValue]) extends Throwable {
		override def toString =
			"On the line $lines you have an array containing incompatible datatypes:" + Json.prettyPrint(JsArray(arr))
	}

	private def collapse(arr: Seq[JsValue]) =
		if (arr.size == 1)
			arr.head
		else try
			arr.foldLeft(JsNull: JsValue)(merge(_, _))
		catch {
			case RowMismatch(_, _) => throw new InconsistentArray(arr)
		}

	private def prepare(arr: JsValue) = arr match {
		case JsArray(x) => JsArray(Seq(collapse(x)))
		case x => x
	}

	def merge(a: JsValue, b: JsValue): JsValue = {
		(a, prepare(b)) match {
			case (JsNull, x) => x
			case (x, JsNull) => x
			case (x: JsBoolean, _: JsBoolean) => x

			case (a@JsString(ax), b@JsString(bx)) =>
				if (ax.size > bx.size) a else b

			case (JsNumber(ax), JsNumber(bx)) => JsNumber((ax max bx) setScale (ax.scale max bx.scale))
			case (JsArray(ax), JsArray(bx)) => JsArray(Seq(merge(ax.head, bx.head)))

			case (JsObject(ax), JsObject(bx)) =>
				JsObject((ax.toSeq ++ bx.toSeq).groupBy(_._1).mapValues {
					case Seq((_, x)) => prepare(x)
					case Seq((_, ax), (_, bx)) => merge(ax, bx)
				}.toSeq)

			case _ => throw new RowMismatch(a, b)
		}
	}



	def out(json: JsValue, i: Int = 0, key: Option[String] = None): String = {
		val pad = "\t" * i
		pad + key.fold("")(_ + " ") + (json match {
			case JsNull => "???"
			case _: JsBoolean => "BOOLEAN"

			case JsString(x) =>
				if ((1 to 65355) contains x.size)
					s"VARCHAR(${x.size})"
				else
					"STRING"

			case JsNumber(x) =>
				if (x.scale == 0) {
					if (x.isValidByte)
						"TINYINT"
					else if (x.isValidShort)
						"SMALLINT"
					else if (x.isValidInt)
						"INT"
					else if (x.isValidLong)
						"BIGINT"
					else
						s"NUMERIC(${x.precision}, 0)"
				} else if (x.precision <= 7)
					"FLOAT"
				else if (x.precision <= 15)
					"DOUBLE"
				else
					"NUMERIC(${x.precision}, ${x.scale})"

			case JsArray(x) =>
				Seq(
					"ARRAY<", out(x.head, i+1), s"$pad>"
				) mkString "\n"

			case JsObject(x) =>
				Seq("STRUCT<") ++ x.map { case (k, v) =>
					out(v, i + 1, Some(k + ":"))
				} ++ Seq(s"$pad>") mkString "\n"
		})
	}

	def definition(i: Int = 0) = schema match {
		case JsObject(x) =>
			x.map {
				case (k, v) => out(v, i, Some(k))
			} mkString ",\n"
	}

	def table(name: String) = Seq(
		"ADD JAR hive-json-serde-0.2.jar;",
		"",
		s"CREATE TABLE $name (",
			definition(1),
		") ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde';",
		"",
		s"LOAD DATA LOCAL INPATH '$file' INTO TABLE $name;"
	) mkString "\n"

	override def toString = table("data")
}
