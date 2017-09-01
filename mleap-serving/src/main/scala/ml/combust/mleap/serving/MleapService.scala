package ml.combust.mleap.serving

import java.io.File

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.DefaultLeapFrame
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.serving.domain.v1.{LoadModelRequest, LoadModelResponse, UnloadModelRequest, UnloadModelResponse}
import ml.combust.mleap.runtime.MleapSupport._
import resource._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.util.{Failure, Success, Try}
import java.io.File

/**
  * Created by hollinwilkins on 1/30/17.
  */
class MleapService()
                  (implicit ec: ExecutionContext) {
  private var bundle: Option[Bundle[Transformer]] = None

  def setBundle(bundle: Bundle[Transformer]): Unit = synchronized(this.bundle = Some(bundle))
  def unsetBundle(): Unit = synchronized(this.bundle = None)

  def getListOfFiles(dir: String):Array[String] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).map(_.getAbsolutePath)
      } else {
          Array[String]()
      }
  }

  def loadModelFromString(model: String) = Future {
    (for(bf <- managed(BundleFile(new File(model)))) yield {
      bf.loadMleapBundle()
    }).tried.flatMap(identity)
  }.flatMap(r => Future.fromTry(r)).andThen {
    case Success(b) => setBundle(b)
  }.map(_ => LoadModelResponse())

  def loadModel(request: LoadModelRequest): Future[LoadModelResponse] = Future {
    (for(bf <- managed(BundleFile(new File(request.path.get.toString)))) yield {
      bf.loadMleapBundle()
    }).tried.flatMap(identity)
  }.flatMap(r => Future.fromTry(r)).andThen {
    case Success(b) => setBundle(b)
  }.map(_ => LoadModelResponse())

  def ping(): Try[String] = {
    if (this.bundle != None) {
      Success("PONG!")
    } else {
      Failure(new IllegalStateException("no transformer loaded"))
    }
  }

  def unloadModel(request: UnloadModelRequest): Future[UnloadModelResponse] = {
    unsetBundle()
    Future.successful(UnloadModelResponse())
  }

  def transform(frame: DefaultLeapFrame): Try[DefaultLeapFrame] = synchronized {
    bundle.map {
      _.root.transform(frame)
    }.getOrElse(Failure(new IllegalStateException("no transformer loaded")))
  }
}
