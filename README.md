Hive SerDe schema generator
==============

With the help of this excellent tool, you are now able to generate a SerDe schema from *.json file, which is in standard SerDe format, e.g. one JSON per line.

The generator is written in Scala, a JVM language, which makes it insanely fast and enables it to run with what you already have installed.

If you like this project, please star or watch it, to support my work and be notfied of future updates.

Example usage (with instructions)
---
First, you have to install SBT (Scala Build Tool), which is the only dependency, besides JDK. You can do that here: http://www.scala-sbt.org/download.html

In the source tree, there is the following JSON in the file `example/users.json`, so we'll use it for demonstration purposes.

```json
{"id":1, "name":"Rok", "income":null, "city":{"name":"Grosuplje", "area":12544}, "children":[{"name":"Matej"}]}
{"id":2, "name":"Jožica", "cars":[], "num":12345678901234.5, "employed":true, "children":null}
{"id":3, "name":"Simon", "num":0.12, "city":{"area":1234.5434}, "children":[{"name":"Simonca"},{"name":"Matic", "toy":"Ropotulica"}]}
```

When we run `sbt "run example/users.json"`, we get the following output. The output can then be either copied or redirected to the output file file.

```sql
ADD JAR hive-json-serde-0.2.jar;

CREATE TABLE data (
	children ARRAY<
		STRUCT<
			toy: VARCHAR(10),
			name: VARCHAR(7)
		>
	>,
	city STRUCT<
		name: VARCHAR(9),
		area: DOUBLE
	>,
	name VARCHAR(6),
	cars ARRAY<
		???
	>,
	num NUMERIC(16, 2),
	employed BOOLEAN,
	id TINYINT,
	income ???
) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde';

LOAD DATA LOCAL INPATH '/home/rok/web/hive-serde-schema-gen/users.json' INTO TABLE data;
```

Take a few moments to examine the result.

You should notice the `???` symbol twice, which means the data type couldn't be inferred from the dataset (the data in every row is either `null` or not present). Therefore it's recommended to check the generated file by hand, before running it.

If you are woundering how we determined the parameters of a `VARCHAR(n)` and `NUMERIC(p, s)` columns, read the next chapter.

Numeric types
===

This schema generator is able to automatically infer the strictest possible data type to store all the values in a column without the loss of information.

If column is integral for all rows (= without the fractional part, like `10`, not like `10.0` or `1.23`), then the generated type is going to be either one of these `TINYINT`, `SMALLINT`, `INT`, `BIGINT`,
or if any of the rows is bigger than that, `NUMERIC(precision, 0)`.

If any of the rows contains number with a fractional part, then the result is either `FLOAT`, `DOUBLE` or if we hit the precision limit of 15 significant digits, then the type is set to `NUMERIC(precision, scale)`.

Exceptions
===
If you feed the program improper data, you will get one of these two exceptions. Both give you the line number and really detailed information.

InconsistentArray
----
SerDe requires your arrays to have the same data type for each element. `["a", {"b":1}]` is thus invalid array, since first element is a `STRING` and the second one is `STRUCT<b: TINYINT>`.

`[1, 12.345]` is however, completely valid, as both of formats are numerical. The result is `ARRAY<FLOAT>` as `FLOAT` suffices to exactly store numbers with up to 7 significant digits.

RowMismatch
---

This exception occurs when we find a line that is not consistent with previous ones. If we have this simple file:

```json
{"names": ["Rok", "Manca"]}
{"names": {"first": "Rok"}}
```

we get this detailed exception:

```
Exception in thread "main" On the line 2 you attempted to insert this JSON:
{
  "first" : "Rok"
}
with corresponding schema:
STRUCT<
	first: VARCHAR(3)
>
into the schema with this signature:
ARRAY<
	VARCHAR(5)
>
```
