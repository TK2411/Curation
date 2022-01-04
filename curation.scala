import scala.util.{Failure, Success, Try}

Try {

  import com.github.music.of.the.ainur.almaren.Almaren
  import org.apache.spark.sql.SaveMode
  import com.modak.common.credential.Credential
  import com.modak.common._
  import com.github.music.of.the.ainur.almaren.builder.Core.Implicit

  val almaren = Almaren("curation-example")

  val args = sc.getConf.get("spark.driver.args").split("\\s+")
  val token = sc.getConf.get("spark.nabu.token")
  val cred_id = args(0).toInt
  val cred_type = args(1).toInt
  val endpoint = sc.getConf.get("spark.nabu.fireshots_url")

  def getCredentiasl(api: String, token: String, credential_id: Int, credential_type_id: Int): ldap = {
    val CredentialResult = Credential.getCredentialData(
      CredentialPayload(token, credential_id, credential_type_id, api)
    )
    val ldap = CredentialResult.data match {
      case ldap: ldap => ldap
      case _ => throw new Exception("Currently unavailable for other credentials Types")
    }
    ldap
  }
  val postCredentials = getCredentiasl(endpoint, token,cred_id, cred_type)


  almaren.builder.sourceJdbc("jdbc:postgresql://w3.training5.modak.com/training_2021","org.postgresql.Driver","select * from mt3035.marketdata",Some(postCredentials.username),Some(postCredentials.password)).sql("select MarketCenter,Arrivals,MinimumPrices,MaximumPrices,ModalPrices from __TABLE__ ").targetJdbc("jdbc:postgresql://w3.training5.modak.com/training_2021","org.postgresql.Driver","mt3035.Transformeddata",SaveMode.Overwrite,Some(postCredentials.username),Some(postCredentials.password)).batch


}match {
  case Success(s) => {
    sys.exit(0)
  }
  case Failure(f) => {
    println(f)
    sys.exit(1)
  }
}