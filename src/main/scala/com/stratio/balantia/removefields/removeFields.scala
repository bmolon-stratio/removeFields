package com.stratio.balantia.removefields

import scala.util.parsing.json.JSON
import org.apache.spark.sql.functions._

class removeFields {

  def removeFieldsFromJsonColumn: (String, String *) => String = (inputJsonString, fieldsToDelete) => {
    val jsonObject = JSON.parseFull(inputJsonString.toString())

    val parsedMap: Map[String, String] = jsonObject match {
      case None => Map.empty
      case _ => jsonObject.get.asInstanceOf[Map[String, String]].filter(x => !fieldsToDelete.contains(x._1))
    }

    scala.util.parsing.json.JSONObject(parsedMap).toString()
  }

  // Función udf

  val cleanJsonUDF = udf(removeFieldsFromJsonColumn)

}
