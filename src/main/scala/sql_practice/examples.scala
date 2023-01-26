package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)
  }

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val peopleDF = spark.read
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")

    val depDF = spark.read
      .csv("data/input/departements.txt")
      .select($"_c0".as("nom_departement"),$"_c1".as("id"))

    peopleDF.agg(sum("Population")).show()

    val orderedpeopleDF = peopleDF.select("Departement", "Population")
      .groupBy("Departement")
      .agg(sum("Population").as("sum_pop"))
      .orderBy($"sum_pop".desc)

    orderedpeopleDF.show()

    orderedpeopleDF.join(depDF, orderedpeopleDF("Departement") === depDF("id"))
      .select("nom_departement", "sum_pop")
      .show()
  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val sample7 = spark.read
      .option("delimiter", "\t")
      .csv("data/input/sample_07")
      .select($"_c0".as("code"),
        $"_c1".as("description"),
        $"_c2".as("total_emp"),
        $"_c3".as("salary"))

    val sample8 = spark.read
      .option("delimiter", "\t")
      .csv("data/input/sample_08")
      .select($"_c0".as("code"),
        $"_c1".as("description"),
        $"_c2".as("total_emp"),
        $"_c3".as("salary"))

    val top_salaries = sample7
      .filter($"salary" > 100000)
      .orderBy($"salary".desc)

    top_salaries.show()

    val growth = sample7
      .join(sample8, sample7.col("code") === sample8.col("code"))
      .select(sample7.col("description"),
        sample7.col("salary").as("salary7"),
        sample8.col("salary").as("salary8"),
        (sample8.col("salary")-sample7.col("salary")).as("growth"))
      .orderBy($"growth".desc)

    growth.show()

    val job_loss = sample7
      .join(sample8, sample7.col("code") === sample8.col("code"))
      .select(sample7.col("description"),
        sample7.col("total_emp").as("emp7"),
        sample8.col("total_emp").as("emp8"),
        (sample8.col("total_emp")-sample7.col("total_emp")).as("job_dif"))
      .filter(sample7.col("salary") > 100000
        || sample8.col("salary") > 100000)
      .orderBy("job_dif")

    job_loss.show()
  }
}
