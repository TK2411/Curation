


import scala.util.{Failure, Success, Try}
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit;
import com.github.music.of.the.ainur.almaren.Almaren;
import com.modak.common.credential.{Credential}
import com.modak.common._
import org.apache.log4j.{Level, Logger, LogManager}
val logger = LogManager.getLogger("com.Artifact")
logger.setLevel(Level.INFO)
val api = sc.getConf.contains("spark.nabu.fireshots_url") match {
  case true => sc.getConf.get("spark.nabu.fireshots_url")
  case false => logger.error(s"Nabu Fireshots URL config is not available")
    sys.exit(1)
}
val token = sc.getConf.contains("spark.nabu.token") match {
  case true => sc.getConf.get("spark.nabu.token")
  case false => logger.error(s"Nabu Token config is not available")
    sys.exit(1)
}
 


val credentialIdJbdc = 980

val credentialTypeIdJbdc = 1

val credentialIdAws = 123

val credentialTypeIdAws = 2


Try {
  def getCredentials(api: String, token: String, credential_id: Int, credential_type_id: Int): CredentialsResult = {
    Credential.getCredentialData(
      CredentialPayload(token, credential_id, credential_type_id, api)
    )
  }
  def getJdbcCredentials(credentialResult: CredentialsResult): ldap = {

    val ldap = credentialResult.data match {

      case ldap: ldap => ldap

      case _ => throw new Exception("Currently unavailable for other credentials Types")

    }

    ldap

  }



  def getAwsCredentials(credentialResult: CredentialsResult): aws = {

    val aws = credentialResult.data match {

      case aws: aws => aws

      case _ => throw new Exception("Currently unavailable for other credentials Types")

    }

    aws

  }

  println(getCredentials(api, token, credentialIdJbdc, credentialTypeIdJbdc))

  val jdbcCredentials = getJdbcCredentials(getCredentials(api, token, credentialIdJbdc, credentialTypeIdJbdc))

  val awsCredentials = getAwsCredentials(getCredentials(api, token, credentialIdAws, credentialTypeIdAws))
  
  println(awsCredentials)



  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "")

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsCredentials.access_id)

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsCredentials.secret_access_key)

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")

  spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")

  spark.conf.set("spark.speculation", "false")

  spark.conf.set("spark.sql.parquet.filterPushdown", "true")

  spark.conf.set("spark.sql.parquet.mergeSchema", "false")





  val almaren = Almaren("nabu-sparkbot-ingestion");

  val df=almaren.builder.

    sourceJdbc("jdbc:oracle:thin:@w3.testorcl.modak.com:1521:orcl", "oracle.jdbc.OracleDriver","""SELECT to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,cast(NUMBER_TYPE as varchar(255)) as number_type,cast(TINYINT_TYPE_1 as varchar(255)) as tinyint_type_1,cast(TINYINT_TYPE_2 as varchar(255)) as tinyint_type_2,cast(SMALLINT_TYPE_3 as varchar(255)) as smallint_type_3,cast(SMALLINT_TYPE_4 as varchar(255)) as smallint_type_4,cast(INT_TYPE_5 as varchar(255)) as int_type_5,cast(INT_TYPE_9 as varchar(255)) as int_type_9,cast(BIGINT_TYPE_15 as varchar(255)) as bigint_type_15,cast(BIGINT_TYPE_18 as varchar(255)) as bigint_type_18,cast(NUMBER_TYPE_31 as varchar(255)) as number_type_31,cast(NUMBER_TYPE_38 as varchar(255)) as number_type_38,cast(NUMBER_TYPE_7_4 as varchar(255)) as number_type_7_4,cast(NUMBER_TYPE_13_7 as varchar(255)) as number_type_13_7,cast(NUMBER_TYPE_23_5 as varchar(255)) as number_type_23_5,cast(NUMBER_TYPE_38_37 as varchar(255)) as number_type_38_37 from NABU_SWAT_DBSRV.ORACLE_NUMBER""",Some(jdbcCredentials.username), Some(jdbcCredentials.password))
    .batch.write.format("parquet").mode("overwrite").option("path", "s3a://cdpmodakbucket/cdpdevenv/data/warehouse0/tablespace/external/hive/test/oracle_test.parquet").save()
 


} match {

  case Success(s) => {

    logger.info(s"Ingestion Success")

    sys.exit(0)

  }

  case Failure(f) => {

    logger.error(s"Error while ingestion", f)

    sys.exit(1)

  }

}

