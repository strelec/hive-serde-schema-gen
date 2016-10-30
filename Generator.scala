import schemer.Schemer

object Generator extends App {
	println(args match {
		case Array(file) =>
			new Schemer(file).table("data")
		case Array(file, tableName) =>
			new Schemer(file).table(tableName)
		case _ =>
			"USAGE: java Generator sample.json [table_name]"
	})
}
