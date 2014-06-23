import schemer.Schemer

object Generator {
	def main(args: Array[String]) {
		println(args.lift(0) match {
			case Some(file) => {
				val schemer = new Schemer(file)
				val tableName = args.lift(1).getOrElse("data")
				schemer.table(tableName)
			}
			case None => "USAGE: java Generator sample.json [table_name]"
		})
	}
}