package com.ubs.frs.eds.tool.transform

import org.apache.spark.sql.{SparkSession,DataFrame}

class SqlMetadata {
   var sql: String = null
   var saveAsTable = true
}
    
class SqlDataFrame extends TFDataFrame {
        
    override def createDataMetadata(): Any = {
        return new SqlMetadata()
    }
    
    override def isValid(metadata: DataTransformMetadata): Boolean = {
        var sqlMetadata = getSqlMetadata(metadata)
        return sqlMetadata.sql != null && sqlMetadata.sql.length > 0
    }
   
    override def createDataFrame(session: TransformSession, 
          metadata: DataTransformMetadata, parameters: Map[String, Any]): DataFrame = {
        val id = metadata.id
        var sqlMetadata =getSqlMetadata(metadata)
        var sql = sqlMetadata.sql
        sql = replaceParameter(parameters, sql)
        var df = session.spark.sql(sql)
        if(id != null)
        	df = df.as(id)
        saveAsTable = sqlMetadata.saveAsTable
        return df
    }
    
    def getSqlMetadata(metadata: DataTransformMetadata): SqlMetadata = {
        return metadata.dataMetadata.asInstanceOf[SqlMetadata]
    }
}



''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


package com.ubs.frs.eds.tool.transform

import org.apache.spark.sql.{SparkSession, DataFrame}

class DataTransformMetadata extends TransformMetadata {
    var id: String = null
    var targetTable: String = null
    var saveMode: String = "overwrite"
    var externalTable: Boolean = false
    var dataMetadata: Any = null
}

class DataTransform(val tfDataFrame: TFDataFrame, val name: String) extends Transform {
    
    override def createMetadata(): TransformMetadata =  {
        val metadata = new DataTransformMetadata()
        metadata.dataMetadata = tfDataFrame.createDataMetadata();
        return metadata
    }
    
    override def isValid(): Boolean = {
        return tfDataFrame.isValid(getDataTransformMetadata())
    }
    
    override def setPrevTransform(transform: Transform): Unit = {
        if(transform.isInstanceOf[DataTransform]) {
        	val dataTranforms: DataTransform = transform.asInstanceOf[DataTransform]
        } 
    }
    
    def saveTable(session: TransformSession, dataframe: DataFrame): Unit = {
        val dataMetadata = getDataTransformMetadata()
        var targetTable = dataMetadata.targetTable
        if(targetTable == null) {
            targetTable = session.getAutoWorkTableName(name)
        }
            
        var saveMode = dataMetadata.saveMode
        if(saveMode == null) {
            saveMode = "overwrite"
        }
   
        if((dataMetadata.externalTable || saveMode != "overwrite") && dataMetadata.targetTable != null) {
            dataframe.write.mode(saveMode).insertInto(targetTable)
        } else {
            dataframe.write.mode(saveMode).saveAsTable(targetTable)
        }
    }
    
    def run(session: TransformSession): Unit = {
        val dataMetadata = getDataTransformMetadata()
        val df = tfDataFrame.createDataFrame(session, dataMetadata, parameters)
        if(tfDataFrame.saveAsTable)
        	  saveTable(session, df)
    }
    
    def getDataTransformMetadata(): DataTransformMetadata = {
    	return metadata.asInstanceOf[DataTransformMetadata]
    }
    
    def getDataMetadata[T](): T = {
        return getDataTransformMetadata().dataMetadata.asInstanceOf[T]
    }
}


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


package com.ubs.frs.eds.tool.transform

trait ConfigTransform {
    var configProperties: Map[String, String] = null
}



package com.ubs.frs.eds.tool.transform

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession

class ConfigDataTransform(override val tfDataFrame: TFDataFrame, override val name: String) 
         extends DataTransform(tfDataFrame, name) with ConfigTransform  {
    
    override def run(session: TransformSession): Unit = {
        val dataMetadata = getDataTransformMetadata()
        var df = tfDataFrame.createDataFrame(session, dataMetadata, parameters)
        df.collect
        var row = df.first
        var map = collection.mutable.Map[String, String]()
        df.schema.fields.foreach(field => {
            var name: String = field.name
            var value: String = row.getAs(field.name).toString()
            map(name) = value
        })
        configProperties = Map(map.toSeq:_*)
    }
}



''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''



package com.ubs.frs.eds.tool.transform

import scala.collection.mutable.ListBuffer
import scala.xml.{Node, NodeSeq}

object XmlParser {
    def setListValueToMap(map: scala.collection.mutable.Map[String, Any],
                    mapKey: String, list: ListBuffer[String]) : List[String] = {
        val newList = List(list.toSeq:_*)
        map(mapKey) = newList
        return newList
    }
        
    def setListAnyToMap(map: scala.collection.mutable.Map[String, Any],
                    mapKey: String, list: ListBuffer[Any]) : List[Any] = {
        val newList = List(list.toSeq:_*)
        map(mapKey) = newList
        return newList
    }
    
    def addMapAnyToList(list: ListBuffer[Any],
                   map: scala.collection.mutable.Map[String, Any]) 
           : Map[String, Any] = {
        val newMap = Map(map.toSeq:_*)
        list += newMap
        return newMap
    }    
    
    def setMapAnyToMap(map: scala.collection.mutable.Map[String, Any],
                    mapKey: String, mapAny: scala.collection.mutable.Map[String, Any]) 
           : Map[String, Any] = {
        val newMap = Map(mapAny.toSeq:_*)
        map(mapKey) = newMap
        return newMap
    }
    
    def loadValueToMap(map: scala.collection.mutable.Map[String, Any],
                       parent: Node, tagName: String, mapKey: String): String = {
        if(parent == null)
            return null
        val value = XmlParser.getElementText(parent, tagName)
        if(value != null)
            map(mapKey) = value
        return value
    }
    
    def loadListValueToMap(map: scala.collection.mutable.Map[String, Any],
                       parent: Node, tagName: String, mapKey: String): List[String] = {
        if(parent == null)
            return null
        val list: ListBuffer[String] = ListBuffer[String]()
        val nodeSeq = parent \ tagName
       	for (node <- nodeSeq) {
           list += node.text
        }
        return setListValueToMap(map, mapKey, list)
    }
    
    def loadMap(map: scala.collection.mutable.Map[String, Any],
                       parent: Node, tagName: String) {
        if(parent == null)
            return
        val nodeSeq = parent \ tagName
        for (node <- nodeSeq) {
            val name = getElementText(node, "Name")
            val valueNode = getElement(node, "Value")
            val value = loadItem(valueNode)
            if(name != null && value != null) {
                map(name) = value
            }
        }
    }
    
    def loadItem(node: Node):Any = {
        val itemSeq = node \ "Item"
        if(itemSeq == null || itemSeq.length == 0){
            return node.text
        } else {
            var map: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
            var list: ListBuffer[Any] = ListBuffer[Any]()
            for(itemNode <- itemSeq) {
                var itemName = getElementText(itemNode, "Name")
                if(itemName == null) {
                    list += loadItem(itemNode)    
                } else {
                   val valueNode = getElement(itemNode, "Value")
                   var itemValue = loadItem(valueNode)                  
                   if(itemValue != null) 
                        map(itemName) = itemValue
                }
            }
            if(!map.isEmpty) 
                return Map(map.toSeq:_*)
            else if(!list.isEmpty)
                return List(list.toSeq:_*) 
        }
        return null
    }
    
    def getElementTextList(parent: Node, nodeName: String): List[String] = {
        if(parent == null)
            return null
        val list: ListBuffer[String] = ListBuffer[String]()
        val nodeSeq = getElements(parent, nodeName)
       	for (node <- nodeSeq) {
           list += node.text
        }
        return List(list.toSeq:_*)
    }       
    
    def getElements(parent: Node, nodeName: String): NodeSeq = {
        return parent \ nodeName
    }
    
    def getElement(parent: Node, nodeName: String): Node = {
        val nodeSeq = parent \ nodeName
        for (node <- nodeSeq) {
            return node
        }
        return null
    }
    
    def getElementText(parent: Node, nodeName: String): String = {
        val node = getElement(parent, nodeName)
        if(node == null)
            return null
        else
            return node.text
    }
}


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''













package com.ubs.frs.eds.tool.transform

import org.apache.spark.sql.{SparkSession,Column,SaveMode}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

        /*
         * measure
         * trend_mtd
         * trend_qtd
         * trend_ytd
         * trend_prior_month_mtd
         * trend_prior_month_qtd
         * trend_prior_month_ytd
         * trend_prior_quarter_mtd
         * trend_prior_quarter_qtd
         * trend_prior_quarter_ytd
         * trend_prior_year_mtd
         * trend_prior_year_qtd
         * trend_prior_year_ytd
         * trend_full_year
         */
class TrendMonthDataFrame extends TrendDataFrame {
    override def trendColumnName = "COB_DATE"
    
    val windowQuarter = Window.partitionBy("KEY","QUARTER").orderBy("MONTH").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val windowYear = Window.partitionBy("KEY","YEAR").orderBy("MONTH").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val windowFYear = Window.partitionBy("KEY","YEAR").orderBy("MONTH").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val windowPrevQuarter = Window.partitionBy("KEY").orderBy("MONTH").rowsBetween(-3,-1)
    val windowQuarterYear = Window.partitionBy("KEY","YEAR").orderBy("MONTH").rowsBetween(Window.unboundedPreceding,-1)
    val windowPrevYear = Window.partitionBy("KEY").orderBy("MONTH").rowsBetween(-12,-1)
    val windowMonth = Window.partitionBy("KEY").orderBy("MONTH")
    
    override def getTrendJoinTableName(session: TransformSession, metadata: DataTransformMetadata, parameters: Map[String, Any]): String = {
        val trendMetadata = getTrendMetadata(metadata)
        val dataStartMth = getMapValue[String](parameters, "DATA_START_MTH", null)
        val startMth = getMapValue[String](parameters, "START_MTH", null)
        val endMth = getMapValue[String](parameters, "END_MTH", null)
        val df = session.spark.sql(s"""
                     SELECT $trendColumnName, 
                         SUBSTR($trendColumnName,1,6) MONTH,
                         SUBSTR($trendColumnName,1,4) YEAR,
                         CASE
                              WHEN SUBSTR($trendColumnName,5,2) IN ('01','02','03') THEN CONCAT(SUBSTR($trendColumnName,1,4), '01')
                              WHEN SUBSTR($trendColumnName,5,2) IN ('04','05','06') THEN CONCAT(SUBSTR($trendColumnName,1,4), '02')
                              WHEN SUBSTR($trendColumnName,5,2) IN ('07','08','09') THEN CONCAT(SUBSTR($trendColumnName,1,4), '03')
                              ELSE CONCAT(SUBSTR($trendColumnName,1,4), '04') END QUARTER,
                         CASE
                              WHEN SUBSTR($trendColumnName,5,2) IN ('01','04','07','10') THEN 1
                              WHEN SUBSTR($trendColumnName,5,2) IN ('02','05','08','11') THEN 2
                              WHEN SUBSTR($trendColumnName,5,2) IN ('03','06','09','12') THEN 3
                              ELSE 0 END QUARTER_MTH,
                         CAST( SUBSTR($trendColumnName,5,2) AS INT ) YEAR_MTH,
                         CASE
                              WHEN SUBSTR($trendColumnName,5,2) IN ('01','02','03') THEN 1
                              WHEN SUBSTR($trendColumnName,5,2) IN ('04','05','06') THEN 2
                              WHEN SUBSTR($trendColumnName,5,2) IN ('07','08','09') THEN 3
                              ELSE 4 END YEAR_QUARTER
                         FROM (SELECT DISTINCT COB_MONTH_LAST_DAY_STR $trendColumnName 
                               FROM GOVERNED.EDS_DATE_MASTER
                               WHERE COB_MONTH_LAST_DAY_STR BETWEEN '$dataStartMth' AND '$endMth')
        """)
        val tableNameJoin = session.getWorkTableName(metadata.name, "JOIN")
        df.write.mode(SaveMode.Overwrite).saveAsTable(tableNameJoin)
        return tableNameJoin
    }
    
    override def getTrendJoinOn(trendStatementAlia: String, primaryTrendColumnName: String, parameters: Map[String, Any]) : String = {
    	return  trendStatementAlia + ".MONTH = SUBSTR(" + primaryTrendColumnName + ",1,6)"
    }
    
    override def getTrendWhere(trendStatementAlia: String, primaryTrendColumnName: String, parameters: Map[String, Any]) : String = {
    	val startMth = getMapValue[String](parameters, "START_MTH", null)
        val endMth = getMapValue[String](parameters, "END_MTH", null)
    	return s"WHERE ${trendStatementAlia}.${trendColumnName} BETWEEN '$startMth' AND '$endMth'"
    }  
       
    override def getTrendTypeCalColumn(trendName: String, trendType: String, amountColumnName: String): Column = {
        if(trendName == null || trendName == "T0") {
            return col(amountColumnName)
        } else if(trendName == "MTD") {
            if(trendType == "Additive") {
                return col(amountColumnName)
            } else if(trendType == "NoneAdditive") {
                return col(amountColumnName)
            } else if(trendType == "Average") {
                return col(amountColumnName)
            } else if(trendType == "Average2") {
                return (coalesce(lag(col(amountColumnName),1).over(windowMonth),lit(0.0)) + col(amountColumnName))/2
            } else if(trendType == "Annual") {
                return col(amountColumnName)*12
            }
        } else if(trendName == "QTD") {
            if(trendType == "Additive") {
                return sum(col(amountColumnName)).over(windowQuarter)
            } else if(trendType == "NoneAdditive") {
                return col(amountColumnName)
            } else if(trendType == "Average") {
                return sum(col(amountColumnName)).over(windowQuarter)/col("QUARTER_MTH")
            } else if(trendType == "Average2") {
                return (coalesce(sum(col("QUARTER_END_AMOUNT")).over(windowPrevQuarter),lit(0.0)) + col(amountColumnName))/2
            } else if(trendType == "Annual") {
                return sum(amountColumnName).over(windowQuarter) * 12 /col("QUARTER_MTH")
            }
        } else if(trendName == "YTD") {
            if(trendType == "Additive") {
                return sum(col(amountColumnName)).over(windowYear)
            } else if(trendType == "NoneAdditive") {
                return col(amountColumnName)
            } else if(trendType == "Average") {
                return sum(col(amountColumnName)).over(windowYear)/col("YEAR_MTH")
            } else if(trendType == "Average2") {
                return (coalesce(sum(col("YEAR_END_AMOUNT")).over(windowPrevYear), lit(0.0)) / 2 + col(amountColumnName) / 2 + coalesce(sum(col("QUARTER_END_AMOUNT")).over(windowQuarterYear),lit(0.0)))/col("YEAR_QUARTER")
            } else if(trendType == "Annual") {
                return sum(amountColumnName).over(windowYear) * 12 /col("YEAR_MTH")
            }
        } else if(trendName == "FY") {
            if(trendType == "Additive") {
                return sum(col(amountColumnName)).over(windowFYear)
            } else if(trendType == "NoneAdditive") {
                return last(col(amountColumnName)).over(windowFYear)
            } else if(trendType == "Average") {
                return sum(col(amountColumnName)).over(windowFYear)/12
            } else if(trendType == "Average2") {
                return  sum(col(amountColumnName)).over(windowFYear)/12
            } else if(trendType == "Annual") {
                return sum(amountColumnName).over(windowFYear) 
            }
        }
        return null
    }
}

















'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''



package com.ubs.frs.eds.tool.transform

import org.apache.spark.sql.{SparkSession,Column,SaveMode}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

class TrendDateDataFrame extends TrendDataFrame {
    override def trendColumnName = "COB_DATE"
  
    var windowWeek5 = Window.partitionBy("KEY","WORK").orderBy(trendColumnName).rowsBetween(-4,0)
    var windowWeek4 = Window.partitionBy("KEY","WORK").orderBy(trendColumnName).rowsBetween(-3,0)
    var windowWeek3 = Window.partitionBy("KEY","WORK").orderBy(trendColumnName).rowsBetween(-2,0)
    var windowWeek2 = Window.partitionBy("KEY","WORK").orderBy(trendColumnName).rowsBetween(-1,0)
    var windowMonth = Window.partitionBy("KEY","MONTH").orderBy(trendColumnName).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    var windowQuarter = Window.partitionBy("KEY","QUARTER").orderBy(trendColumnName).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    var windowYear = Window.partitionBy("KEY","YEAR").orderBy(trendColumnName).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    var windowFYear = Window.partitionBy("KEY","YEAR")
    var windowWork = Window.partitionBy("KEY","WORK").orderBy(trendColumnName)
    var window21Work = Window.partitionBy("KEY","WORK").orderBy(trendColumnName).rowsBetween(-20,0)
    val windowPrevQuarter = Window.partitionBy("KEY").orderBy(trendColumnName).rowsBetween(-3,-1)
    val windowQuarterYear = Window.partitionBy("KEY","YEAR").orderBy(trendColumnName).rowsBetween(Window.unboundedPreceding,-1)
    val windowPrevYear = Window.partitionBy("KEY").orderBy(trendColumnName).rowsBetween(-12,-1)
    
    override def getTrendJoinTableName(session: TransformSession, metadata: DataTransformMetadata, parameters: Map[String, Any]): String = {
        val trendMetadata = getTrendMetadata(metadata)
        val dataStartDate = getMapValue[String](parameters, "DATA_START_DATE", null)
        val startDate = getMapValue[String](parameters, "START_DATE", null)
        val endDate = getMapValue[String](parameters, "END_DATE", null)
        val dfDate = session.spark.sql(s"""
                     SELECT $trendColumnName, 
                         SUBSTR($trendColumnName,1,6) MONTH,
                         SUBSTR($trendColumnName,1,4) YEAR,
                         CASE
                              WHEN SUBSTR($trendColumnName,5,2) IN ('01','02','03') THEN CONCAT(SUBSTR($trendColumnName,1,4), '01')
                              WHEN SUBSTR($trendColumnName,5,2) IN ('04','05','06') THEN CONCAT(SUBSTR($trendColumnName,1,4), '02')
                              WHEN SUBSTR($trendColumnName,5,2) IN ('07','08','09') THEN CONCAT(SUBSTR($trendColumnName,1,4), '03')
                              ELSE CONCAT(SUBSTR($trendColumnName,1,4), '04') END QUARTER,
                         CASE
                              WHEN SUBSTR($trendColumnName,5,2) IN ('01','04','07','10') THEN 1
                              WHEN SUBSTR($trendColumnName,5,2) IN ('02','05','08','11') THEN 2
                              WHEN SUBSTR($trendColumnName,5,2) IN ('03','06','09','12') THEN 3
                              ELSE 0 END QUARTER_MTH,
                         CAST( SUBSTR($trendColumnName,5,2) AS INT ) YEAR_MTH,
                         CASE
                              WHEN SUBSTR($trendColumnName,5,2) IN ('01','02','03') THEN 1
                              WHEN SUBSTR($trendColumnName,5,2) IN ('04','05','06') THEN 2
                              WHEN SUBSTR($trendColumnName,5,2) IN ('07','08','09') THEN 3
                              ELSE 4 END YEAR_QUARTER,
                         CASE WHEN BUSINESS_DAY > 0 THEN 1
                              ELSE 0 END WORK,
                         BUSINESS_DAY MONTH_WORK_DAYS,   
                         MONTH_BUSINESS_DAY TOTAL_MONTH_WORK_DAYS,
                         CASE WHEN BUSINESS_DAY = MONTH_BUSINESS_DAY THEN MONTH_BUSINESS_DAY
                              ELSE 0 END MONTH_END_BUSINESS_DAY
                         FROM (SELECT COB_DATE_STR $trendColumnName, BUSINESS_DAY
                               FROM GOVERNED.EDS_DATE_MASTER
                               WHERE COB_DATE_STR BETWEEN '$startDate' AND '$endDate')
                         JOIN (SELECT SUBSTR(COB_DATE_STR,1,6) TMONTH, MAX(BUSINESS_DAY) MONTH_BUSINESS_DAY
                               FROM GOVERNED.EDS_DATE_MASTER
                               WHERE COB_DATE_STR BETWEEN '$dataStartDate' AND '$endDate'
                               GROUP BY SUBSTR(COB_DATE_STR,1,6))
                          ON SUBSTR($trendColumnName,1,6) = TMONTH
                         
        """)
        val tableNameDate = session.getWorkTableName(metadata.name, "DATE")
        dfDate.write.mode(SaveMode.Overwrite).saveAsTable(tableNameDate)
        
        val trendWindowWeek = Window.orderBy(trendColumnName)
        val trendWindowQuarter = Window.partitionBy("QUARTER").orderBy(trendColumnName).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        val trendWindowFQuarter = Window.partitionBy("QUARTER").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        val trendWindowYear = Window.partitionBy("YEAR").orderBy(trendColumnName).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        val trendWindowFYear = Window.partitionBy("YEAR").orderBy(trendColumnName).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        
        val df = session.spark.table(tableNameDate).
        withColumn("WEEK_WORK_DAYS", when(col("MONTH_WORK_DAYS") === lit(0), lit(0)).
                                     when(lag(col("MONTH_WORK_DAYS"),1).over(trendWindowWeek) === lit(0), lit(1)).
                                     when(lag(col("MONTH_WORK_DAYS"),2).over(trendWindowWeek) === lit(0), lit(2)).
                                     when(lag(col("MONTH_WORK_DAYS"),3).over(trendWindowWeek) === lit(0), lit(3)).
                                     when(lag(col("MONTH_WORK_DAYS"),4).over(trendWindowWeek) === lit(0), lit(4)).
                                     when(lag(col("MONTH_WORK_DAYS"),5).over(trendWindowWeek) === lit(0), lit(5)).
                                     when(lead(col("MONTH_WORK_DAYS"),1).over(trendWindowWeek) === lit(0), lit(5)).
                                     when(lead(col("MONTH_WORK_DAYS"),2).over(trendWindowWeek) === lit(0), lit(4)).
                                     when(lead(col("MONTH_WORK_DAYS"),3).over(trendWindowWeek) === lit(0), lit(3)).
                                     when(lead(col("MONTH_WORK_DAYS"),4).over(trendWindowWeek) === lit(0), lit(2)).
                                     when(lead(col("MONTH_WORK_DAYS"),5).over(trendWindowWeek) === lit(0), lit(1)).
                                     otherwise(lit(0))).
        withColumn("QUARTER_WORK_DAYS", col("MONTH_WORK_DAYS") + sum("MONTH_END_BUSINESS_DAY").over(trendWindowQuarter)).
        withColumn("TOTAL_QUARTER_WORK_DAYS", sum("MONTH_END_BUSINESS_DAY").over(trendWindowFQuarter)).
        withColumn("YEAR_WORK_DAYS", col("MONTH_WORK_DAYS") + sum("MONTH_END_BUSINESS_DAY").over(trendWindowYear)).
        withColumn("TOTAL_YEAR_WORK_DAYS", sum("MONTH_END_BUSINESS_DAY").over(trendWindowFYear)).
        drop("MONTH_END_BUSINESS_DAY")
        val tableNameJoin = session.getWorkTableName(metadata.name, "JOIN")
        df.write.mode(SaveMode.Overwrite).saveAsTable(tableNameJoin)
        return tableNameJoin
    }
    
    override def getTrendJoinOn(trendStatementAlia: String, primaryTrendColumnName: String, parameters: Map[String, Any]) : String = {
    	return  trendStatementAlia + "." + trendColumnName + " = " + primaryTrendColumnName 
    } 
    
    override def getTrendWhere(trendStatementAlia: String, primaryTrendColumnName: String, parameters: Map[String, Any]) : String = {
        val startDate = getMapValue[String](parameters, "START_DATE", null)
        val endDate = getMapValue[String](parameters, "END_DATE", null)
    	return s"WHERE ${trendStatementAlia}.MONTH_WORK_DAYS > 0 OR $primaryTrendColumnName IS NOT NULL AND ${trendStatementAlia}.${trendColumnName} BETWEEN '$startDate' AND '$endDate'"
    }    
       
    override def getTrendTypeCalColumn(trendName: String, trendType: String, amountColumnName: String): Column = {
        if(trendName == null || trendName == "T0") {
            return col(amountColumnName)
        } else if(trendName == "T1") {
            return coalesce( when(col("WORK") === lit("1"), lag(col(amountColumnName),1).over(windowWork)),lit(0.0))
        } else if(trendName == "T2") {
            return coalesce( when(col("WORK") === lit("1"), lag(col(amountColumnName),2).over(windowWork)),lit(0.0))
        } else if(trendName == "T3") {
            return coalesce( when(col("WORK") === lit("1"), lag(col(amountColumnName),3).over(windowWork)),lit(0.0))
        } else if(trendName == "LAST_T1") {
            return coalesce( when(col("WORK") === lit("1"), sum(col(amountColumnName)).over(window21Work)/lit("21")),lit(0.0))
        } else if(trendName == "WTD") {
            if(trendType == "Additive") {
                return when(col("WORK") === lit("1"), sum(col(amountColumnName)).over(windowWeek5))
            } else if(trendType == "NoneAdditive") {
                return col(amountColumnName)
            } else if(trendType == "Average") {
                return when(col("WEEK_WORK_DAYS") === lit(5),  sum(amountColumnName).over(windowWeek5) / col("WEEK_WORK_DAYS")).
                       when(col("WEEK_WORK_DAYS") === lit(4),  sum(amountColumnName).over(windowWeek4) / col("WEEK_WORK_DAYS")).
                       when(col("WEEK_WORK_DAYS") === lit(3),  sum(amountColumnName).over(windowWeek3) / col("WEEK_WORK_DAYS")).
                       when(col("WEEK_WORK_DAYS") === lit(2),  sum(amountColumnName).over(windowWeek2) / col("WEEK_WORK_DAYS")).
                       when(col("WEEK_WORK_DAYS") === lit(1),  col(amountColumnName)).
                       otherwise(lit(0.0))
            } else if(trendType == "Average2") {
                return lit(0.0)
            } else if(trendType == "Annual") {
                return when(col("WEEK_WORK_DAYS") === lit(5),  sum(amountColumnName).over(windowWeek5) * lit(260) / col("WEEK_WORK_DAYS")).
                       when(col("WEEK_WORK_DAYS") === lit(4),  sum(amountColumnName).over(windowWeek4) * lit(260) / col("WEEK_WORK_DAYS")).
                       when(col("WEEK_WORK_DAYS") === lit(3),  sum(amountColumnName).over(windowWeek3) * lit(260) / col("WEEK_WORK_DAYS")).
                       when(col("WEEK_WORK_DAYS") === lit(2),  sum(amountColumnName).over(windowWeek2) * lit(260) / col("WEEK_WORK_DAYS")).
                       when(col("WEEK_WORK_DAYS") === lit(1),  col(amountColumnName) * col("TOTAL_YEAR_WORK_DAYS")).
                       otherwise(lit(0.0))
            }
        } else if(trendName == "MTD") {
            if(trendType == "Additive") {
                return sum(col(amountColumnName)).over(windowMonth)
            } else if(trendType == "NoneAdditive") {
                return col(amountColumnName)
            } else if(trendType == "Average") {
                return when(col("MONTH_WORK_DAYS") > 0, sum(col(amountColumnName)).over(windowMonth)/col("MONTH_WORK_DAYS")).
                       otherwise(lit(0.0))
            } else if(trendType == "Average2") {
                return lit(0.0)
            } else if(trendType == "Annual") {
                return when(col("MONTH_WORK_DAYS") > 0,  sum(amountColumnName).over(windowMonth) * col("TOTAL_MONTH_WORK_DAYS") * lit(12) / col("MONTH_WORK_DAYS")).
                       otherwise(lit(0.0))
            }
        } else if(trendName == "QTD") {
            if(trendType == "Additive") {
                return sum(col(amountColumnName)).over(windowQuarter)
            } else if(trendType == "NoneAdditive") {
                return col(amountColumnName)
            } else if(trendType == "Average") {
                return when(col("MONTH_WORK_DAYS") > 0, sum(col(amountColumnName)).over(windowQuarter)/col("QUARTER_WORK_DAYS")).
                       otherwise(lit(0.0))
            } else if(trendType == "Average2") {
                return lit(0.0)
            } else if(trendType == "Annual") {
               return when(col("MONTH_WORK_DAYS") > 0,  sum(amountColumnName).over(windowQuarter) * col("TOTAL_QUARTER_WORK_DAYS") * lit(4) / col("QUARTER_WORK_DAYS")).
                      otherwise(lit(0.0))
            }
        } else if(trendName == "YTD") {
            if(trendType == "Additive") {
                return sum(col(amountColumnName)).over(windowYear)
            } else if(trendType == "NoneAdditive") {
                return col(amountColumnName)
            } else if(trendType == "Average") {
                return when(col("MONTH_WORK_DAYS") > 0, sum(col(amountColumnName)).over(windowYear)/col("YEAR_WORK_DAYS")).
                       otherwise(lit(0.0))
            } else if(trendType == "Average2") {
                return lit(0.0)
            } else if(trendType == "Annual") {
                return when(col("MONTH_WORK_DAYS") > 0,  sum(amountColumnName).over(windowYear) * col("TOTAL_YEAR_WORK_DAYS") / col("YEAR_WORK_DAYS")).
                       otherwise(lit(0.0))            
            }
        } else if(trendName == "FY") {
            if(trendType == "Additive") {
                return sum(col(amountColumnName)).over(windowFYear)
            } else if(trendType == "NoneAdditive") {
                return last(col(amountColumnName)).over(windowFYear)
            } else if(trendType == "Average") {
                return sum(col(amountColumnName)).over(windowFYear)/12
            } else if(trendType == "Average2") {
                return lit(0.0)
            } else if(trendType == "Annual") {
                return sum(amountColumnName).over(windowFYear) 
            }
        }
        return null
    }
}


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''




package com.ubs.frs.eds.tool.transform

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{SparkSession,DataFrame,Column,RelationalGroupedDataset,SaveMode}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

class TrendMetadata {
    var sourceTable: String = null
    var sourceTrendColumnName: String = null
    var amountColumnName: String = null
    var keyColumnNames: List[String] = null
    var where: String = null
    var defaultTrendType: String = null
    var trendTypeColumnName: String = null
    var trendTypes: List[TrendType] = null
    var trendColumns: List[TrendColumn] = null
    var pivotColumn: String = null
    var pivotValues: List[String] = null
    var excludeOutputColumnNames: List[String] = null
    var outputColumns: List[String] = null
    var skipZeroAmount: Boolean = true
    
    def addTrendType(typeName: String, values: List[String]) {
        val trendType = new TrendType(typeName, values)
        if(trendTypes == null) {
            trendTypes = List[TrendType](trendType)
        } else{ 
            var newTrendTypes: ListBuffer[TrendType] = ListBuffer(trendTypes.toSeq:_*)
            newTrendTypes += trendType
            trendTypes = List[TrendType](newTrendTypes.toSeq:_*)
        }  
    }
    
    def addTrendColumn(trendName: String, columnName: String) {
        val trendColumn = new TrendColumn(trendName, columnName)
        if(trendColumns == null) {
            trendColumns = List[TrendColumn](trendColumn)
        } else{ 
            var newTrendColumns: ListBuffer[TrendColumn] = ListBuffer(trendColumns.toSeq:_*)
            newTrendColumns += trendColumn
            trendColumns = List[TrendColumn](newTrendColumns.toSeq:_*)
        }  
    }
    
    class TrendType(val typeName: String, val values: List[String]) {
    }
    
    class TrendColumn(val trendName: String, val columnName: String) {
    }
}
    
abstract class TrendDataFrame extends TFDataFrame {
    def trendColumnName: String
    def getTrendTypeCalColumn(trendName: String, trendType: String, amountColumnName: String): Column
    def getTrendJoinTableName(session: TransformSession, metadata: DataTransformMetadata, parameters: Map[String, Any]): String
    def getTrendJoinOn(trendStatementAlia: String, primaryTrendColumnName: String, parameters: Map[String, Any]) : String
    def getTrendWhere(trendStatementAlia: String, primaryTrendColumnName: String, parameters: Map[String, Any]) : String
    
    override def createDataMetadata(): Any = {
    	  return new TrendMetadata()
    }
    
    override def isValid(metadata: DataTransformMetadata): Boolean = {
        var trendMetadata = getTrendMetadata(metadata)
        return (trendMetadata.trendTypeColumnName != null || trendMetadata.defaultTrendType != null) && trendMetadata.keyColumnNames != null
    }
  
    override def createDataFrame(session: TransformSession, 
          metadata: DataTransformMetadata, parameters: Map[String, Any]): DataFrame = {
        val trendMetadata = getTrendMetadata(metadata)
        val buckets = 120
        val trendTypeNames = List("Additive","NoneAdditive","Average","Average2","Annual")
        val keySeparater = "\f"
        val KeyNull = "\r"
        val PivotSeparater = "\r\r"
        
        var sourceTrendColumnName = trendMetadata.sourceTrendColumnName
        if(sourceTrendColumnName == null)
            sourceTrendColumnName = trendColumnName
        var amountColumnName = trendMetadata.amountColumnName
        if(amountColumnName == null)
            amountColumnName = "MEASURE_AMOUNT"
        val trendTypeColumnName = trendMetadata.trendTypeColumnName
        var defaultTrendType = trendMetadata.defaultTrendType
        
        val keys = trendMetadata.keyColumnNames
        var where = trendMetadata.where
        var sourceTableName = trendMetadata.sourceTable
        if(sourceTableName == null)
            sourceTableName = session.lastAutoTableName;
        
        val pivotColumn = trendMetadata.pivotColumn
        val pivotValues = trendMetadata.pivotValues
        val excludeOutputColumnNames = trendMetadata.excludeOutputColumnNames
        var outputColumns = trendMetadata.outputColumns
        val skipZeroAmount = trendMetadata.skipZeroAmount
        
        val keyColumns: ListBuffer[Column] = new ListBuffer()
        val defaultOutputColumns: ListBuffer[Column] = new ListBuffer()
        defaultOutputColumns += col(trendColumnName)
        
        if(pivotColumn != null) {
            keyColumns += when(col(pivotColumn).isNotNull, col(pivotColumn)).otherwise(lit(KeyNull))
            keyColumns += lit(PivotSeparater)
        }
        
        var bTrendTypeColumnInKey = false
        keys foreach {
            var index = 0
            key => {
                if(index > 0)
                    keyColumns += lit(keySeparater)
                keyColumns += when(col(key).isNotNull, col(key)).otherwise(lit(KeyNull))
                defaultOutputColumns += split(col("KEY"), keySeparater)(index).as(key)
                index = index + 1
                if(key == trendTypeColumnName)
                    bTrendTypeColumnInKey = true
            }
        }
        if(!bTrendTypeColumnInKey && trendTypeColumnName != null) {
            keyColumns += lit(keySeparater) 
            keyColumns += when(col(trendTypeColumnName).isNotNull, col(trendTypeColumnName)).otherwise(lit(KeyNull))
        }
        
        var dfInput = session.spark.table(sourceTableName)
        if(where != null) {
            where = replaceParameter(parameters, where)  
            dfInput = dfInput.where(where)
        }
        dfInput = dfInput.withColumn("KEY",concat(keyColumns.toSeq:_*))
        keys foreach {
            key => {
                dfInput = dfInput.drop(key)
            }
        }
        
        var trendTypeAggMaxColumns = new ListBuffer[Column]();
        if(trendTypeColumnName != null)
            trendTypeAggMaxColumns += max(trendTypeColumnName).as(trendTypeColumnName);
        dfInput = dfInput.groupBy(col(sourceTrendColumnName), col("KEY")).agg(sum(amountColumnName).as("_AMOUNT"), trendTypeAggMaxColumns.toSeq:_*)
     
        val tableNameInput = session.getWorkTableName(metadata.name, "INPUT")
        dfInput.write.mode(SaveMode.Overwrite).bucketBy(buckets, "KEY").saveAsTable(tableNameInput)

        val tableNameKey = session.getWorkTableName(metadata.name, "KEY")
        var dfKey: DataFrame = null
        if(trendTypeColumnName != null) {
            dfKey = session.spark.table(tableNameInput).groupBy("KEY").
                  agg(max(trendTypeColumnName).as(trendTypeColumnName))
        } else {
            dfKey = session.spark.table(tableNameInput).select("KEY").distinct 
        }
        dfKey.write.mode(SaveMode.Overwrite).bucketBy(buckets, "KEY").saveAsTable(tableNameKey)

        var trendJoinTableName = getTrendJoinTableName(session, metadata, parameters)
        
        val tableNameMtd = session.getWorkTableName(metadata.name, "MTD")
        var trendTypeSelectColumnName = ""
        if(trendTypeColumnName != null)
            trendTypeSelectColumnName = s""",TKEY.$trendTypeColumnName"""
        val trendJoinOn = getTrendJoinOn("EDM", "TCURR." + sourceTrendColumnName, parameters) 
        val trendWhere = getTrendWhere("EDM", "TCURR." + sourceTrendColumnName, parameters) 
        val dfMtd = session.spark.sql(s"""
           SELECT
              EDM.*
              ,TKEY.KEY KEY
              $trendTypeSelectColumnName
              ,NVL(TCURR._AMOUNT, 0) $amountColumnName
              ,CASE WHEN SUBSTR(EDM.MONTH,5,2) = '12' THEN NVL(TCURR._AMOUNT, 0)
                   ELSE 0 END YEAR_END_AMOUNT
              ,CASE WHEN SUBSTR(EDM.MONTH,5,2) IN ('03','06','09','12') THEN NVL(TCURR._AMOUNT, 0)
                   ELSE 0 END QUARTER_END_AMOUNT
           FROM $tableNameKey TKEY
           JOIN $trendJoinTableName EDM
           LEFT JOIN $tableNameInput TCURR
             ON $trendJoinOn
               AND TCURR.KEY = TKEY.KEY
             $trendWhere
        """)
        dfMtd.write.mode(SaveMode.Overwrite).bucketBy(buckets, "KEY").saveAsTable(tableNameMtd)

        var dfTrend = session.spark.table(tableNameMtd)
        val trendTypes = trendMetadata.trendTypes
        val trendColumns = trendMetadata.trendColumns
        var trendOutputColumnNames = ListBuffer[String]()
        if(trendColumns != null) {
            trendColumns foreach {
                trendColumn => {
                    val trendName = trendColumn.trendName
                    var columnName = trendColumn.columnName
                    if(columnName == null)
                        columnName = "AMOUNT"
                    var column: Column = null
                    if(trendTypeColumnName == null) {
                        column = getTrendTypeCalColumn(trendName, defaultTrendType, amountColumnName)
                    } else {
                        trendTypes foreach { 
                            trendType => {
                                val values = trendType.values
                                if(values != null) {
                                    var calColumn = getTrendTypeCalColumn(trendName, trendType.typeName, amountColumnName)
                                    if(calColumn != null) {
                                        if(column == null)
                                            column = when(col(trendTypeColumnName).isin(values.toSeq:_*), calColumn)
                                        else
                                            column = column.when(col(trendTypeColumnName).isin(values.toSeq:_*), calColumn)
                                    }
                                }
                            }
                        }
                        if(column != null)
                            column = column.otherwise(lit("0.0"))
                    }
                    if(column == null)
                        column = lit("0.0")
                        
                    dfTrend = dfTrend.withColumn(columnName, column)
                    trendOutputColumnNames += columnName
                }
            }
        }
        
        dfTrend = dfTrend.drop("YEAR_END_AMOUNT").drop("QUARTER_END_AMOUNT").drop("YEAR").drop("MONTH").drop("QUARTER").drop("QUARTER_MTH").drop("YEAR_MTH").drop("YEAR_QUARTER")    
      
        if(pivotColumn != null) {
            dfTrend = dfTrend.withColumn(pivotColumn, split(col("KEY"), PivotSeparater)(0))
                             .withColumn("PKEY", split(col("KEY"), PivotSeparater)(1))
                             .drop("KEY")
        }

        val tableNameTrend = session.getWorkTableName(metadata.name, "TREND")        
        dfTrend.write.mode(SaveMode.Overwrite).saveAsTable(tableNameTrend)
        
        
        var tableNameOutput: String = null
        var outputWhere: String = null
        if(pivotColumn != null) {
            var firstPivotAggColumn: Column = null
            var pivotAggColumns = ListBuffer[Column]()
            trendOutputColumnNames foreach {
               
                
                columnName => {
                    if(firstPivotAggColumn == null)
                        firstPivotAggColumn = sum(columnName).alias(columnName)
                    else
                        pivotAggColumns += sum(columnName).alias(columnName)
                }
            }
            var dfPivot = session.spark.table(tableNameTrend).
                   groupBy(trendColumnName,"PKEY").
                   pivot(pivotColumn, pivotValues).
                   agg(firstPivotAggColumn, pivotAggColumns.toSeq:_*).withColumnRenamed("PKEY", "KEY")
            pivotValues foreach {
                pivotValue => {
                    trendOutputColumnNames foreach {
                        columnName => {
                            val pivotAmountColumnName = pivotValue + "_" + columnName
                            dfPivot = dfPivot.withColumn(pivotAmountColumnName, coalesce(col(pivotAmountColumnName), lit(0.0)))
                            if(excludeOutputColumnNames == null || !excludeOutputColumnNames.contains( pivotAmountColumnName)) {
                            	defaultOutputColumns += col(pivotAmountColumnName)
                             	if(outputWhere == null)
                					outputWhere = pivotAmountColumnName + " <> 0"
                				else
                    				outputWhere = outputWhere + " OR " + pivotAmountColumnName + " <> 0"
                    	    }
                        }
                    }   
                }
            }
                                
            val tableNamePivot = session.getWorkTableName(metadata.name, "PIVOT")        
            dfPivot.write.mode(SaveMode.Overwrite).saveAsTable(tableNamePivot)
            
            tableNameOutput = tableNamePivot
        } else {
            tableNameOutput = tableNameTrend
            trendOutputColumnNames foreach {
                columnName => {
                 	if(excludeOutputColumnNames == null || !excludeOutputColumnNames.contains(columnName)) {
                	    defaultOutputColumns += col(columnName)
                		if(outputWhere == null)
                			outputWhere = columnName + " <> 0"
                		else
                    		outputWhere = outputWhere + " OR " + columnName + " <> 0"
                    }
            	}
            }
        }
   
        var dfOut = session.spark.table(tableNameOutput)
        if(skipZeroAmount && outputWhere != null) {
        	dfOut = dfOut.where(outputWhere)
        	println("outputWhere:" + outputWhere)
        }
        dfOut = dfOut.select(defaultOutputColumns.toSeq:_*)
      	
        keys foreach {
        	columnName => {
        		dfOut = dfOut.withColumn(columnName, when(col(columnName) === lit(KeyNull), lit(null)).otherwise(col(columnName)))
        	}
        }
        
        if(outputColumns != null) {
            var selectOutputColumns = ListBuffer[Column]()
            
            outputColumns foreach {
                outputColumn => selectOutputColumns += expr(outputColumn)
            }
            dfOut = dfOut.select(selectOutputColumns.toSeq:_*)
        }
        
        return dfOut
    }
    
     def getTrendMetadata(metadata: DataTransformMetadata): TrendMetadata = {
        return metadata.dataMetadata.asInstanceOf[TrendMetadata]
    }   
}



''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


package com.ubs.frs.eds.tool.transform

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession

class TransformSessionMetadata(val appName: String, val sessionMetadataName: String) {
    var configTransform: Transform = null
    val transforms: ListBuffer[Transform] = ListBuffer()
    
    def config(transform: Transform): Unit =  {
        configTransform = transform;
    }
  
    def add(transform: Transform): Unit = {
        transforms += transform
    }
}

class TransformSession(sessionMetadata: TransformSessionMetadata, sessionName: String) {
    val prefixWorkTableName = "ETL.WORK_TRANSFORM_"
    var spark: SparkSession = null
    var sessionProperties = Map[String, String]()
    var lastAutoTableName: String = null
    var tableNameIndex = 0
    val workTableList: ListBuffer[String] = ListBuffer[String]()
    
    def loadConfig(args: Array[String]) {
        var map = collection.mutable.Map[String, String]()
        if(args != null) {
            args foreach {
                arg => {
                    val index = arg.indexOf("=")
                    val key = arg.substring(0, index)
                    val value = arg.substring(index + 1)
                    if(key != null && value != null)
                        map(key) = value  
                }
            }
        }
        sessionProperties = Map(map.toSeq:_*)
    }
  
    def run() : Unit = {
        println("start " + sessionName)
        /*spark = SparkSession.
              builder().
              appName(sessionName).
              enableHiveSupport().
              getOrCreate()*/
      System.setProperty("HADOOP_USER_NAME", "edsd")
      spark = SparkSession.builder()
        .appName("test")
        .master("yarn")
        .config("spark.yarn.jars", "hdfs://a301-9019-4819.zur.swissbank.com:8020/user/paramek/jars/*.jar")
        .enableHiveSupport()
        .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.crossJoin.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions","120")
        spark.conf.set("hive.metastore.try.direct.sql","true")
    
        println("config session")
        
        try {
            val configTransform = sessionMetadata.configTransform
            if(configTransform != null && configTransform.isValid()) {
                println("execute config transform")
                configTransform.runTransform(this, null)
                if(configTransform.isInstanceOf[ConfigTransform]) {
                    val configTran = configTransform.asInstanceOf[ConfigTransform]
                    println("configTransform: " + configTran.configProperties)
                    if(configTran.configProperties != null) {
                        val props = scala.collection.mutable.Map(configTran.configProperties.toSeq:_*)
                        if(sessionProperties != null) {
                            for((key, value) <- sessionProperties) {
                                props(key) = value
                            }
                        }
                        sessionProperties = Map(props.toSeq:_*)
                    }
                }
            }
            println("sessionProperties:" + sessionProperties)
            var prevTransform: Transform = null
            sessionMetadata.transforms foreach {
                transform => {
                    if(transform.isValid()) {
                        println("execute transform: " + transform.metadata.name)
                        transform.runTransform(this, prevTransform)
                        prevTransform = transform  
                    }
                }
            }
            println("end " + sessionName)
        } finally {
            workTableList foreach {
                tableName => {
                  println("drop table if exists " + tableName)
                    spark.sql("DROP TABLE IF EXISTS " + tableName)
                }
            }
        }
    }
    
    def getWorkTableName(name: String, actionName: String): String = {
        var tableName: String = prefixWorkTableName + sessionMetadata.appName.toUpperCase + "_" + sessionName.toUpperCase + "_" + name.toUpperCase + "_" + actionName.toUpperCase
        workTableList += tableName
        return tableName
    }  
    
    def getAutoWorkTableName(name: String): String = {
        tableNameIndex = tableNameIndex + 1
        lastAutoTableName = getWorkTableName(name, "AUTO" + tableNameIndex)
        return lastAutoTableName
    }
}


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''



package com.ubs.frs.eds.tool.transform

import org.apache.spark.sql.SparkSession

trait TransformMetadata {
    var name: String = null
}

trait Transform {
    val name: String 
    val metadata: TransformMetadata = createMetadata()
    metadata.name = name
    var parameters: Map[String, Any] = null
    
    var setParameters: (Map[String, String], scala.collection.mutable.Map[String,Any]) => Unit = null
    
    def createMetadata(): TransformMetadata
    def isValid(): Boolean
    def run(session: TransformSession): Unit
    
    def setPrevTransform(transform: Transform): Unit = {
    }
    
    def runTransform(session: TransformSession, prevTransform: Transform): Unit = {
        setPrevTransform(prevTransform)
        val sessionProperties = session.sessionProperties     
        var param = scala.collection.mutable.Map[String,Any]()
        if(parameters != null)
            param = scala.collection.mutable.Map[String,Any](parameters.toSeq:_*)
        if(setParameters != null) { 
           setParameters(sessionProperties, param)
        }
        var param2 = scala.collection.mutable.Map[String,Any]()
        for ((key,value) <- param) {
            var newValue = value
            if(newValue != null && newValue.isInstanceOf[String]) {
                var strValue = newValue.asInstanceOf[String]
                var sindex = strValue.indexOf("$Session{")
            	while(sindex >= 0) {
            		var eindex = strValue.indexOf("}", sindex)
            		if(eindex > 0) {
            	    	var sessionKey = strValue.substring(sindex + 9, eindex)
            	    	if(sessionProperties.contains(sessionKey)) {
            	        	var sessionValue = sessionProperties(sessionKey)
            	        	strValue = strValue.substring(0, sindex) + sessionValue + strValue.substring(eindex + 1)
            	    	} else {
            	    	    Console.err.println("Fail to retreive sessionKey: " + sessionKey)
            	    	}
            		}
            		sindex = strValue.indexOf("$Session{", sindex + 1)
            	}
            	newValue = strValue
            }
            param2(key) = newValue
        }
        parameters = Map[String,Any](param2.toSeq:_*)
     
        run(session)
    }
}


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


package com.ubs.frs.eds.tool.transform

import org.apache.spark.sql.{SparkSession,DataFrame}
import scala.util.control._

trait TFDataFrame {
    
    var saveAsTable: Boolean = true
    
    def createDataMetadata(): Any
    def isValid(metadata: DataTransformMetadata): Boolean
    
    def createDataFrame(session: TransformSession, 
           metadata: DataTransformMetadata, parameters: Map[String, Any]): DataFrame
    
    def getMapValue[T](map: Map[String, Any], key: String, defaultVal: T): T = {
        if(map == null)
            return defaultVal
        if(!map.contains(key)) 
            return defaultVal
         var rtn: T = defaultVal
         var anyRtn = map(key)
         if(anyRtn != null) 
             rtn = anyRtn.asInstanceOf[T]
         return rtn
    }
    
    def replaceParameter(parameters: Map[String, Any], value: String): String = {
        if(parameters == null || value == null)
            return value
        var newValue = value
        var sindex = newValue.indexOf("${")
        while(sindex >= 0) {
            var eindex = newValue.indexOf("}", sindex)
            if(eindex > 0) {
            	val paramKey = newValue.substring(sindex + 2, eindex)
            	val strValue = replaceValue(parameters, paramKey,
                        newValue.substring(0, sindex), newValue.substring(eindex + 1))
                if(strValue != null)
                    newValue = strValue
            }
            sindex = newValue.indexOf("${", sindex + 1)
        }
        sindex = newValue.indexOf("$")
        while(sindex >= 0) {
            var num = 0
            val loop = new Breaks; 
            loop.breakable {
                for (i <- sindex + 1 until newValue.length) {
                    var c = newValue.charAt(i)
                    if ((c >= 'a' && c <= 'z') ||
                        (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') ||
                         c == '_' || c > '~') {
                        num += 1 
                    } else {
                        loop.break
                    }
                }
            }
            if(num == 0) {
                sindex = -1
            } else {
                val eindex = sindex + 1 + num
                val paramKey = newValue.substring(sindex + 1, eindex)
                val strValue = replaceValue(parameters, paramKey,
                        newValue.substring(0, sindex), newValue.substring(eindex))
                if(strValue != null)
                    newValue = strValue
                sindex = newValue.indexOf("$", sindex + 1)
            }
           
        }
        return newValue
    }
    
    def replaceValue(parameters: Map[String, Any], paramKey: String,
               firstPart: String, secondPart: String): String = {
        if(!parameters.contains(paramKey)) 
            return null
        var paramValue = parameters(paramKey)
        var strValue = paramValue.asInstanceOf[String]
        if(strValue == null)
            return null
        return firstPart + strValue + secondPart
    }
}


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


package com.ubs.frs.eds.tool.transform

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{SparkSession,DataFrame,Column,RelationalGroupedDataset}
import org.apache.spark.sql.functions.expr

class TableMetadata {
    var sourceTable: String = null
    var selectColumns: List[String] = null
    var where: String = null
    var joins: List[Join] = null
    var groupBy: Group = null
    
    def addJoin(dataMetadata: DataTransform, 
                joinOn: String, 
                joinType: String = null) {
        val join = new Join(dataMetadata, joinOn, joinType)
        if(joins == null) {
            joins = List[Join](join)
        } else{ 
            var newJoins: ListBuffer[Join] = ListBuffer(joins.toSeq:_*)
            newJoins += join
            joins = List[Join](newJoins.toSeq:_*)
        } 
    }
    
    def setGroup(groupColumns: List[String],
        		 aggColumns: List[String],
        		 pivotColumn: String = null,
        		 pivotValues: List[String] = null) {
        groupBy = new Group(groupColumns, aggColumns, pivotColumn, pivotValues)
    }   
    
    class Join (val dataTransform: DataTransform,
                val joinOn: String,
                val joinType: String) {
    }
    
    class Group (val groupColumns: List[String],
        		 val aggColumns: List[String],
        		 val pivotColumn: String,
        		 val pivotValues: List[String]){
    }
}
    
class TableDataFrame extends TFDataFrame {

    override def createDataMetadata(): Any = {
    	  return new TableMetadata()
    }
    
    override def isValid(metadata: DataTransformMetadata): Boolean = {
        var tableMetadata = getTableMetadata(metadata)
        return tableMetadata.selectColumns != null || tableMetadata.where != null || 
               tableMetadata.joins != null || tableMetadata.groupBy != null
    }
    
    override def createDataFrame(session: TransformSession, 
          metadata: DataTransformMetadata, parameters: Map[String, Any]): DataFrame = {
        var tableMetadata = getTableMetadata(metadata)
        
        val id = metadata.id
        var table = tableMetadata.sourceTable
        if(table == null)
            table = session.lastAutoTableName

        var df = session.spark.table(table)
        if(id != null)
        	  df = df.as(id)
        
        if(tableMetadata.joins != null) {
            tableMetadata.joins foreach {
                join => {
                    if(join.dataTransform != null) {
                        var joinOn: String = join.joinOn
                        joinOn = replaceParameter(parameters, joinOn)    
                        val joinOnColumn = expr(joinOn)
                        var joinType = join.joinType
                        if(joinType == null)
                            joinType = "inner"
                        var joinMetadata = join.dataTransform.getDataTransformMetadata()
                        val joinDf = join.dataTransform.tfDataFrame.createDataFrame(session, joinMetadata, parameters)
                        if(join.dataTransform.isValid())
                            df = df.join(joinDf, joinOnColumn, joinType)    
                    }
                }
            }
        }
        
        var where = tableMetadata.where
        if(where != null) {
            where = replaceParameter(parameters, where)  
            df = df.where(where)
        }        

        if(tableMetadata.groupBy != null) {
            val groups = tableMetadata.groupBy.groupColumns
            val aggs = tableMetadata.groupBy.aggColumns
            val groupColumns: ListBuffer[Column] = ListBuffer[Column]()
            groups foreach {
                columnExpr => {
                    val columnExpr2 = replaceParameter(parameters, columnExpr)   
                    groupColumns += expr(columnExpr2)
                }
            }
            val aggColumns: ListBuffer[Column] = ListBuffer[Column]()
            aggs foreach {
                columnExpr => {
                    val columnExpr2 = replaceParameter(parameters, columnExpr)  
                    aggColumns += expr(columnExpr2)
                }
            }
            var gdf: RelationalGroupedDataset = df.groupBy(groupColumns.toSeq:_*)
            val firstColumn = aggColumns(0)
            val restColumns : ListBuffer[Column] = aggColumns.drop(0)
            
            val pivotColumn = tableMetadata.groupBy.pivotColumn
            val pivotValues = tableMetadata.groupBy.pivotValues
            
            if(pivotColumn == null) {
            	  df = gdf.agg(firstColumn, restColumns.toSeq:_*)
            } else {
                if(pivotValues == null)
                    gdf = gdf.pivot(pivotColumn)
                else
            	      gdf = gdf.pivot(pivotColumn, pivotValues.toSeq)
            	  df = gdf.agg(firstColumn, restColumns.toSeq:_*)
            }
        }
        
        val selectColumns = tableMetadata.selectColumns
        if(selectColumns != null) {
            val columns: ListBuffer[Column] = ListBuffer[Column]()
            selectColumns foreach {
                columnExpr => {
                    val columnExpr2 = replaceParameter(parameters, columnExpr)  
                    columns += expr(columnExpr2)
                }
            }
            df = df.select(columns:_*)
        }        
        
        return df
    }
    
    def createJoinDataFrameClass(className: Any): TFDataFrame = {
        val dfClass = className.asInstanceOf[String]
        if(dfClass == null)
            return null
        val tfDfInst  = Class.forName(dfClass).newInstance
        if(tfDfInst == null) 
            return null
       return tfDfInst.asInstanceOf[TFDataFrame]
    }
    
    def getTableMetadata(metadata: DataTransformMetadata): TableMetadata = {
        return metadata.dataMetadata.asInstanceOf[TableMetadata]
    }  
}

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


<job>
	<Name>KARTHIK_TEST</Name>
	<TargetKPI>6030</TargetKPI>
	<InputFilter>SOURCE BETWEEN '20001' AND '20009' OR (SUBSTR(SOURCE,6,1) = '-' AND SUBSTR(SOURCE,1,5) BETWEEN '20001' AND '20009')</InputFilter>
    <TrendType>Annual</TrendType>
</job>

<job>
	<Name>RPT_IB_DAILY_MEASURE_TYPE_LRD_ANNUAL</Name>
	<TargetKPI>6031</TargetKPI>
	<InputFilter>SOURCE BETWEEN '4001' AND '4019' OR (SUBSTR(SOURCE,5,1) = '-' AND SUBSTR(SOURCE,1,4) BETWEEN '4001' AND '4019')</InputFilter>
    <TrendType>Annual</TrendType>
</job>

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''



package com.ubs.frs.eds.provision.ib.daily

import java.io.File
import scala.io.Source
import scala.xml.{XML, Node, NodeSeq}
import com.ubs.frs.eds.tool.transform._

object RptIBDailyTransform {
  def main(args: Array[String]) {
    var xmlFileName: String = null
    if (args.length > 0) {
      xmlFileName = args(0)
    } else {
      println("Input parameter missing")
    }

    var appArgs: Array[String] = null
    if (args.length > 1) {
      appArgs = args.slice(1, args.length)
    }

    println("xmlFileName: " + xmlFileName)
    var canonicalPath = new File(xmlFileName).getCanonicalPath()
    println("xml Canonical Path: " + canonicalPath)

    println("content: ")
    for (line <- Source.fromFile(canonicalPath).getLines) {
      println(line)
    }

    val xml = XML.loadFile(canonicalPath)

    val sessionName = XmlParser.getElementText(xml, "Name")
    val targetKPI = XmlParser.getElementText(xml, "TargetKPI")
    val filter = XmlParser.getElementText(xml, "InputFilter")
    val trendType = XmlParser.getElementText(xml, "TrendType")
    val joinSql = XmlParser.getElementText(xml, "JoinSql")
    val joinOn = XmlParser.getElementText(xml, "JoinOn")
    val joinType = XmlParser.getElementText(xml, "JoinType")

    transform(appArgs, sessionName, targetKPI, trendType, filter, joinSql, joinOn, joinType)
  }

  def transform(appArgs: Array[String], sessionName: String, targetKPI: String, trendType: String, filter: String, joinSql: String, joinOn: String, joinType: String) {

    var sessionMetadata = new TransformSessionMetadata("IB", sessionName)

    var configDT = new ConfigDataTransform(new SqlDataFrame(), null)
    configDT.getDataMetadata[SqlMetadata]().sql = "SELECT CONCAT(SUBSTR(YTD_MINUS_2_LAST_DAY_STR,1,6), '01') DATA_START_DATE, YTD_MINUS_1_FIRST_DAY_STR START_DATE, YTD_LAST_DAY_STR YTD_LAST_DATE, COB_DATE_STR COB_DATE FROM GOVERNED.EDS_BATCH_DATE_FILE"
    sessionMetadata.config(configDT)

    var input = new DataTransform(new TableDataFrame(), "input")
    input.getDataTransformMetadata().id = "INPUT"
    var tableMetadata = input.getDataMetadata[TableMetadata]()
    tableMetadata.sourceTable = "PROVISION.IB_DAILY_MEASURE_TYPE_KPI"
    var where = "YYYY BETWEEN SUBSTR('${START_DATE}',1,4) and SUBSTR('${END_DATE}',1,4) AND COB_DATE BETWEEN '${START_DATE}' and '${END_DATE}' AND INPUT.MEASURE_AMOUNT <> 0 LIMIT 100"
    if (filter != null)
      where = where + " AND ( " + filter + " )"
    tableMetadata.where = where
    tableMetadata.selectColumns = List("INPUT.COB_DATE", "INPUT.FUNCTION_CODE", "INPUT.PROJECTION_TYPE", "INPUT.LEGAL_ENTITY_ID", "INPUT.SUB_DESK_ID", "INPUT.SUB_DESK_NAME", "INPUT.COUNTRY_CODE", "INPUT.CURRENCY_CODE", "INPUT.TRANSFORMATION_TYPE", "INPUT.MEASURE_AMOUNT", "INPUT.DATA_AS_OF_DATE", "INPUT.YYYY")
    if (joinSql != null) {
      var joinDf = new DataTransform(new SqlDataFrame(), "join")
      joinDf.getDataTransformMetadata().id = "JOIN"
      joinDf.getDataMetadata[SqlMetadata]().sql = joinSql
      input.getDataMetadata[TableMetadata]().addJoin(joinDf, joinOn, joinType)
    }
    input.setParameters = (sessionProperties: Map[String, String],
                           parameters: scala.collection.mutable.Map[String, Any]) => {
      parameters("START_DATE") = sessionProperties("START_DATE")
      parameters("END_DATE") = sessionProperties("YTD_LAST_DATE")
    }
    sessionMetadata.add(input)

    var input2 = new DataTransform(new TableDataFrame(), "input2")
    input2.getDataTransformMetadata().externalTable = true
    input2.getDataTransformMetadata().targetTable = "PROVISION.IB_DAILY_MEASURE_TYPE_KPI"
    var input2Metadata = input2.getDataMetadata[TableMetadata]()
    input2Metadata.selectColumns = List("COB_DATE", "FUNCTION_CODE", "PROJECTION_TYPE", "LEGAL_ENTITY_ID", "SUB_DESK_ID", "SUB_DESK_NAME", "COUNTRY_CODE", "CURRENCY_CODE", targetKPI + " KPI_ID", "TRANSFORMATION_TYPE", "CAST(NULL AS STRING) MEASURE_TYPE", "MEASURE_AMOUNT", "DATA_AS_OF_DATE", "YYYY", "'" + targetKPI + "' SOURCE")
    sessionMetadata.add(input2)

    var repairInput = new DataTransform(new SqlDataFrame(), "repairInput")
    var repairMetadata = repairInput.getDataMetadata[SqlMetadata]()
    repairMetadata.sql = "MSCK REPAIR TABLE PROVISION.IB_DAILY_MEASURE_TYPE_KPI"
    repairMetadata.saveAsTable = false
    sessionMetadata.add(repairInput)

    val targetKpiLength = (targetKPI.length() + 1).toString
    var trend = new DataTransform(new TrendDateDataFrame(), "trend")
    trend.getDataTransformMetadata().targetTable = "PROVISION.RPT_IB_DAILY_MEASURE_TYPE_KPI"
    trend.getDataTransformMetadata().externalTable = true
    var tredMetadata = trend.getDataMetadata[TrendMetadata]()
    tredMetadata.keyColumnNames = List("FUNCTION_CODE", "PROJECTION_TYPE", "LEGAL_ENTITY_ID", "SUB_DESK_ID", "SUB_DESK_NAME", "COUNTRY_CODE", "CURRENCY_CODE", "TRANSFORMATION_TYPE", "DATA_AS_OF_DATE")
    tredMetadata.sourceTable = "PROVISION.IB_DAILY_MEASURE_TYPE_KPI"
    tredMetadata.where = s"SUBSTR(SOURCE,1,$targetKpiLength) in ('" + targetKPI + "', '" + targetKPI + "-') AND YYYY BETWEEN SUBSTR('${DATA_START_DATE}',1,4) and SUBSTR('${END_DATE}',1,4) AND COB_DATE BETWEEN '${DATA_START_DATE}' and '${END_DATE}'"
    tredMetadata.defaultTrendType = trendType
    tredMetadata.addTrendColumn("T0", "T0_AMOUNT")
    tredMetadata.addTrendColumn("WTD", "WTD_AMOUNT")
    tredMetadata.addTrendColumn("MTD", "MTD_AMOUNT")
    tredMetadata.addTrendColumn("QTD", "QTD_AMOUNT")
    tredMetadata.addTrendColumn("YTD", "YTD_AMOUNT")
    tredMetadata.addTrendColumn("FY", "FY_AMOUNT")
    tredMetadata.pivotColumn = "PROJECTION_TYPE"
    tredMetadata.pivotValues = List("ACTUAL", "ESTIMATE", "PLAN", "TARGET")
    tredMetadata.excludeOutputColumnNames = List("ACTUAL_FY_AMOUNT")
    tredMetadata.outputColumns = List("COB_DATE REPORTING_DATE",
      "FUNCTION_CODE",
      "NVL(LEGAL_ENTITY_ID,'UNASSIGNED') LEGAL_ENTITY_ID",
      "NVL(SUB_DESK_ID,'UNASSIGNED') SUB_DESK_ID",
      "NVL(SUB_DESK_NAME,'UNASSIGNED') SUB_DESK_NAME",
      "COUNTRY_CODE",
      "CURRENCY_CODE",
      targetKPI + " KPI_ID",
      "TRANSFORMATION_TYPE",

      "ACTUAL_T0_AMOUNT DTD_ACTUAL",
      "ESTIMATE_T0_AMOUNT DTD_ACTUAL",
      "TARGET_T0_AMOUNT DTD_ACTUAL",
      "PLAN_T0_AMOUNT DTD_ACTUAL",

      "0.0 DTD_T1_ESTIMATE",
      "0.0 DTD_T1_ACTUAL",
      "0.0 DTD_T2_ACTUAL",
      "0.0 DTD_T3_ACTUAL",

      "0.0 DTD_VAR_T1",
      "0.0 DTD_LAST_T1",
      "0.0 DAYS_AVG",
      "0.0 DTD_EST",


      "ACTUAL_WTD_AMOUNT WTD_ACTUAL",
      "ACTUAL_MTD_AMOUNT MTD_ACTUAL",
      "ACTUAL_QTD_AMOUNT QTD_ACTUAL",
      "ACTUAL_YTD_AMOUNT YTD_ACTUAL",

      "PLAN_WTD_AMOUNT WTD_PLAN",
      "PLAN_MTD_AMOUNT MTD_PLAN",
      "PLAN_QTD_AMOUNT QTD_PLAN",
      "PLAN_YTD_AMOUNT YTD_PLAN",

      "TARGET_WTD_AMOUNT WTD_TARGET",
      "TARGET_MTD_AMOUNT MTD_TARGET",
      "TARGET_QTD_AMOUNT QTD_TARGET",
      "TARGET_YTD_AMOUNT YTD_TARGET",

      "PLAN_FY_AMOUNT FY_PLAN",
      "TARGET_FY_AMOUNT FY_TARGET",

      "0.0 PY_MTD_ACTUAL",
      "0.0 PY_QTD_ACTUAL",
      "0.0 PY_YTD_ACTUAL",

      "0.0 DAILY_PPA",
      "0.0 WEEKLY_PPA",

      "DATA_AS_OF_DATE",
      "SUBSTR(COB_DATE,1,4) YYYY",
      "'" + targetKPI + "' SOURCE")

    trend.setParameters = (sessionProperties: Map[String, String],
                           parameters: scala.collection.mutable.Map[String, Any]) => {
      parameters("DATA_START_DATE") = sessionProperties("DATA_START_DATE")
      parameters("START_DATE") = sessionProperties("START_DATE")
      parameters("END_DATE") = sessionProperties("YTD_LAST_DATE")
    }

    sessionMetadata.add(trend)

    val session = new TransformSession(sessionMetadata, sessionMetadata.sessionMetadataName)
    if (appArgs != null)
      session.loadConfig(appArgs)
    session.run()
  }
}
