package cn.ultron.spark.commons

import cn.ultron.spark.commons.util.PropertyUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaSet


/**
  * 该类提供基础的spark执行代码,使开发者只关注核心业务代码即可.
  */
class BaseApp(appName: String) extends Serializable {

    lazy val logger = LoggerFactory.getLogger(classOf[BaseApp])

    /**
      * Spark运行环境参数配置。private[this]:对象私有,不生成getter/setter方法。
      */
    private[this] val SPARK_CONFIG_FILE = "spark-config.properties"

    /**
      * spark参数配置信息以spark.开头
      */
    private[this] val SPARK_CONF_HEADER = "spark."

    /**
      * 初始化sparkconf.
      */
    lazy val sparkConf: SparkConf = {
        val sparkConf = new SparkConf()
        val sparkConfig = new PropertyUtil(SPARK_CONFIG_FILE).properties;
        val sparkMaster = sparkConfig.getProperty("task.spark.master","yarn-cluster")
        if (sparkMaster == null || "".equals(sparkMaster)) {
            logger.warn("spark master is null,will be considered as yarn-cluster")
        } else {
            if (sparkMaster.startsWith("local") || sparkMaster.startsWith("spark") || sparkMaster.equalsIgnoreCase("yarn-client")) {
                sparkConf.setMaster(sparkConfig.getProperty("task.spark.master"))
                if (sparkMaster.equalsIgnoreCase("yarn-client")) {
                    // 设置用户
                    System.setProperty("HADOOP_USER_NAME", sparkConfig.getProperty("task.hadoop.user"));
                }
            }
        }
        sparkConf.setAppName(appName)
        sparkConfig.stringPropertyNames.foreach(
            x => x.startsWith(SPARK_CONF_HEADER) match {
                case true => sparkConf.set(x, sparkConfig.getProperty(x))
            })
        sparkConf;
    }

    /**
      * 初始化sparksession
      */
    lazy val sparkSession: SparkSession = {
        logger.info("初始化SparkSession")
        val sparkSession = SparkSession.builder();
        sparkSession.appName(appName)
        val sparkConfig = new PropertyUtil(SPARK_CONFIG_FILE).properties;
        val sparkMaster = sparkConfig.getProperty("task.spark.master", "yarn-cluster");
        if (sparkMaster == null || "".equals(sparkMaster)) {
            logger.warn("spark master is null,will be considered as yarn-cluster")
        } else {
            if (sparkMaster.startsWith("local") || sparkMaster.startsWith("spark") || sparkMaster.equalsIgnoreCase("yarn-client")) {
                sparkSession.master(sparkConfig.getProperty("task.spark.master"))
                if (sparkMaster.equalsIgnoreCase("yarn-client")) {
                    // 设置用户
                    System.setProperty("HADOOP_USER_NAME", sparkConfig.getProperty("task.hadoop.user"));
                }
            }
        }
        // enable hive
        if (sparkConfig.getProperty("task.spark.hive.enable", "false").toBoolean) {
            sparkSession.enableHiveSupport();
        }
        sparkConfig.stringPropertyNames.foreach(
            x => x.startsWith(SPARK_CONF_HEADER) match {
                case true => sparkSession.config(x, sparkConfig.getProperty(x))
                case false=>Unit
            })
        sparkSession.getOrCreate();
    }

}

object BaseApp {
    def main(args: Array[String]) {
        var app = new BaseApp("test");
        app.sparkSession.sparkContext.textFile("hdfs://192.168.2.66:8020/bda-edge/conf/app.properties").foreach(println)
    }
}