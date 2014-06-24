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
		override def toString = "On the line " + lines + " you attempted to insert this JSON:" + "\n" + Json.prettyPrint(b) +
			"\n" + "with corresponding schema:" + "\n" + out(b) +
			"\n" + "into the schema with this signature:" + "\n" + out(a)
	}

	case class InconsistentArray(arr: Seq[JsValue]) extends Throwable {
		override def toString = "On the line " + lines + " you have an array containing incompatible datatypes:" +
			Json.prettyPrint(JsArray(arr))
	}

	private def collapse(arr: Seq[JsValue]) = if (arr.size == 1) arr.head else try
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
			case (a@JsString(ax), b@JsString(bx)) => if (ax.size > bx.size) a else b
			case (JsNumber(ax), JsNumber(bx)) => JsNumber((ax max bx) setScale (ax.scale max bx.scale))
			case (JsArray(ax), JsArray(bx)) => JsArray(Seq(merge(ax.head, bx.head)))
			case (JsObject(ax), JsObject(bx)) => JsObject((ax ++ bx).groupBy(_._1).mapValues {
				case Seq((_, x)) => prepare(x)
				case Seq((_, ax), (_, bx)) => merge(ax, bx)
			}.toSeq)
			case _ => throw new RowMismatch(a, b)
		}
	}



	def out(json: JsValue, i: Int = 0, key: Option[String] = None): String = {
		val pad = "\t" * i
		pad + key.map(_ + " ").getOrElse("") + (json match {
			case JsNull => "???"
			case _: JsBoolean => "BOOLEAN"
			case JsString(x) => if ((1 to 65355) contains x.size) "VARCHAR(" + x.size + ")" else "STRING"
			case JsNumber(x) =>
				if (x.scale == 0) {
					if (x.isValidByte) {
						"TINTYINT"
					} else if (x.isValidShort) {
						"SMALLINT"
					} else if (x.isValidInt) {
						"INT"
					} else if (x.isValidLong) {
						"BIGINT"
					} else {
						"NUMERIC(" + x.precision + ", 0)"
					}
				} else if (x.precision <= 7) {
					"FLOAT"
				} else if (x.precision <= 15) {
					"DOUBLE"
				} else {
					"NUMERIC(" + x.precision + ", " + x.scale + ")"
				}
			case JsArray(x) => "ARRAY<\n" + out(x.head, i+1) + "\n" + pad + ">"
			case JsObject(x) => "STRUCT<\n" + x.map { case (k, v) =>
				out(v, i + 1, Some(k + ":"))
			}.mkString(",\n") + "\n" + pad + ">"
		})
	}

	def definition(i: Int = 0) = schema match {
		case JsObject(x) => x.map { case (k, v) =>
			out(v, i, Some(k))
		} mkString ",\n"
	}

	def table(name: String) = {
		"ADD JAR hive-json-serde-0.2.jar;" + "\n\n" +
		s"CREATE TABLE $name (" + "\n" +
			definition(1) + "\n" +
		") ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde';" + "\n\n" +
		s"LOAD DATA LOCAL INPATH '$file' INTO TABLE $name;"
	}

	override def toString = table("data")
}