import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j._

object SparkOptimizedWrapper {
  def main(args: Array[String]): Unit  = {

    
    val logger = Logger.getRootLogger


    val conf = new SparkConf()
    val sparkWrapperSession = SparkSession.builder()
      .config(conf)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.orc.default.buffer.size", "131072")
      .enableHiveSupport()
      .getOrCreate()

    val sc = sparkWrapperSession.sparkContext

    logger.info("Wrapper Application Id framework " + sc.applicationId)
    

    //Read job plan execution map to Array.

    logger.info(" Reading job plan from file: " + args(0))
    logger.warn(" Reading job plan from file: " + args(0))
    val job_plan_meta_info = scala.io.Source.fromFile(args(0)).getLines().filter(!_.isEmpty).map(line => line.split(",").map(_.trim)).toArray

    //Reading the param file into a Map.
    logger.info(" Reading the job parameters from file: " + args(1))
    logger.warn(" Reading the job parameters from file: " + args(1))
    val sourceParameters = scala.io.Source.fromFile(args(1)).getLines().filter(!_.isEmpty).map(line => line.split(":").map(_.trim)).flatMap(t => Map(t(0) -> t(1))).toMap


    //Recursive parameter assignment function to identify all parameters in hql and replace them with values provided in parameter file.
    def paramAssignment(hql_location: String): String = {

      logger.info(" Reading the hql from file: " + hql_location)
      logger.warn(" Reading the hql from file: " + hql_location)

      val hql_txt: String = scala.io.Source.fromFile(hql_location).mkString
      
      logger.info(" Assigning parameters for hql in file: " + hql_location)
      logger.warn(" Assigning parameters for hql in file: " + hql_location)

      //Searching in finding all the variables in the hql_txt. The searchString identifies all words starting with $$.
      val searchString = "\\$\\$\\w*".r
      val paramList = searchString.findAllIn(hql_txt).toList
      //To replace the parameters, building the escape sequence required to handle $$
      val replaceStringPrefix = "\\$\\$"

      // Replacing parameters with actual values based on provided parameter file. Uses efficient tail-recursion.
      def replace_recursion(i: Int, acc: String, sourceParameters: Map[String, String]): String = {
        if (i > paramList.length - 1) acc
        else {
          val paramKey = paramList(i).substring(2)
          val paramValue = sourceParameters(paramKey)
          replace_recursion(i + 1, acc.replaceAll(replaceStringPrefix.concat(paramKey), paramValue), sourceParameters)
        }
      }

      replace_recursion(0, hql_txt, sourceParameters)
    }

    //Evaluating and executing the hql. Reads the location of hql and calls paramAssignment function to assign parameters.
    //Post parameter assignment, hql is evaluated and executed as required.
    def createTT(TempTableRequired: String, Cached: String, Broadcast: String, TempTableName: String, hql_txt: String) {
      if (TempTableRequired == "Y") {
        if (Cached == "Y") {

          logger.info(" Evaluating hql for cached temporary table: " + TempTableName)
          logger.warn(" Evaluating hql for cached temporary table: " + TempTableName)

          sparkWrapperSession.sql(hql_txt).cache().createOrReplaceTempView(TempTableName)

          logger.info(" Completed evaluating hql for temporary table: " + TempTableName)
          logger.warn(" Completed evaluating hql for temporary table: " + TempTableName)
        }
        else {

          logger.info(" Evaluating hql for temporary table: " + TempTableName)
          logger.warn(" Evaluating hql for temporary table: " + TempTableName)

          sparkWrapperSession.sql(hql_txt).createOrReplaceTempView(TempTableName)

          logger.info(" Completed evaluating hql for temporary table: " + TempTableName)
          logger.warn(" Completed evaluating hql for temporary table: " + TempTableName)
        }
        if (Broadcast == "Y") {
          sc.broadcast(TempTableName)

        }
      }
      else {

        logger.info(" Evaluating hql for action step. If evaluation is ok, job execution will start immediately.")
        logger.warn(" Evaluating hql for action step. If evaluation is ok, job execution will start immediately.")

        sparkWrapperSession.sql(hql_txt)

        logger.info(" Job Succeeded")
        logger.warn(" Job Succeeded")
      }
    }

    //loop to process all lines of input in sequence
    for (i <- job_plan_meta_info.indices) {
      createTT(job_plan_meta_info(i)(1), job_plan_meta_info(i)(2), job_plan_meta_info(i)(3), job_plan_meta_info(i)(4), paramAssignment(job_plan_meta_info(i)(5)))
    }

    sc.stop()
    sparkWrapperSession.stop()
  } //closing main
}
//closing object
