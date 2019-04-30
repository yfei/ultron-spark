package cn.ultron.spark.commons.util;

import java.io.IOException;
import java.util.Properties;
import java.io.FileInputStream;
import org.slf4j.LoggerFactory

class PropertyUtil(fileName: String) extends Serializable {

  lazy val logger = LoggerFactory.getLogger(classOf[PropertyUtil]);

  val properties = {
    logger.info("读取配置文件" + fileName);
    val classpath = classOf[PropertyUtil].getResource("/").getPath();
    var properties = new Properties();
    properties.load(new FileInputStream(classpath + fileName));
    properties;
  }
}