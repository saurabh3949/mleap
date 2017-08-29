package ml.combust.mleap.serving

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import ml.combust.mleap.serving.domain.v1.LoadModelRequest


/**
  * Created by hollinwilkins on 1/30/17.
  */
object MleapServer extends ExtensionId[MleapServer]
  with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): MleapServer = {
    new MleapServer(system.settings.config)(system)
  }

  override def lookup(): ExtensionId[_ <: Extension] = MleapServer
}

class MleapServer(tConfig: Config)
                 (implicit val system: ExtendedActorSystem) extends Extension {
  import system.dispatcher
  implicit val materializer = ActorMaterializer()

  val config = MleapConfig(tConfig.getConfig("ml.combust.mleap.serving"))

  val service = new MleapService()
  val files = service.getListOfFiles("/opt/ml/model")
  if (!files.isEmpty) {
    service.loadModelFromString(files(0))
    println("Loaded model successfully", files(0))
  }

  val resource = new MleapResource(service)
  val routes = resource.routes

  for(model <- config.model) {
    service.loadModel(LoadModelRequest().withPath(model))
  }

  Http().bindAndHandle(routes, config.http.bindHostname, config.http.bindPort)
}
