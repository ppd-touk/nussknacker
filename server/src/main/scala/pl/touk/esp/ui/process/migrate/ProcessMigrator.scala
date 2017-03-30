package pl.touk.esp.ui.process.migrate

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.Materializer
import argonaut.DecodeJson
import cats.data.EitherT
import cats.implicits._
import pl.touk.esp.ui.EspError
import pl.touk.esp.ui.process.ProcessToSave
import pl.touk.esp.ui.process.displayedgraph.DisplayableProcess
import pl.touk.esp.ui.process.marshall.UiProcessMarshaller
import pl.touk.esp.ui.process.repository.ProcessRepository.{InvalidProcessTypeError, ProcessDetails}
import pl.touk.esp.ui.security.LoggedUser
import pl.touk.esp.ui.util.ProcessComparator.Difference
import pl.touk.esp.ui.util.{Argonaut62Support, ProcessComparator}
import pl.touk.esp.ui.validation.ValidationResults.{ValidationErrors, ValidationResult}

import scala.concurrent.{ExecutionContext, Future}

trait ProcessMigrator {

  def targetEnvironmentId: String

  def compare(localProcess: DisplayableProcess)(implicit ec: ExecutionContext) : Future[Either[EspError, List[Difference]]]

  def migrate(localProcess: DisplayableProcess)(implicit ec: ExecutionContext, loggedUser: LoggedUser) : Future[Either[EspError, Unit]]

}

case class MigratorCommunicationError(getMessage: String) extends EspError

case class MigratorValidationError(errors: ValidationErrors) extends EspError {
  override def getMessage = {
    val messages = errors.globalErrors.map(_.message) ++
      errors.processPropertiesErrors.map(_.message) ++ errors.invalidNodes.map { case(node, nerror) => s"$node - ${nerror.map(_.message).mkString(", ")}"}
    s"Cannot migrate, following errors occured: ${messages.mkString(", ")}"
  }
}

case class HttpMigratorTargetEnvironmentConfig(url: String, user: String, password: String, environmentId: String)


class HttpProcessMigrator(config: HttpMigratorTargetEnvironmentConfig, val environmentId: String)(implicit as: ActorSystem, val materializer: Materializer) extends StandardProcessMigrator {

  override def targetEnvironmentId = config.environmentId

  val http = Http()

  override protected def request(path: String, method: HttpMethod, request: MessageEntity): Future[HttpResponse] = {
    http.singleRequest(HttpRequest(uri = s"${config.url}/$path", method = method, entity = request,
      headers = List(Authorization(BasicHttpCredentials(config.user, config.password)))))
  }
}

trait StandardProcessMigrator extends Argonaut62Support with ProcessMigrator {

  def environmentId: String

  implicit def materializer: Materializer

  import argonaut.ArgonautShapeless._
  import pl.touk.esp.ui.codec.UiCodecs._

  val uiProcessMarshaller = UiProcessMarshaller()

  private def invoke[T:DecodeJson](path: String, method: HttpMethod, requestEntity: RequestEntity = HttpEntity.Empty)(implicit ec: ExecutionContext)
    : Future[Either[EspError, T]]= {
    request(path, method, requestEntity).flatMap { response =>
      if (response.status.isSuccess()) {
        Unmarshal(response.entity).to[T].map[Either[EspError, T]](Right(_))
      } else {
        Unmarshaller
          .stringUnmarshaller(response.entity).map(error => Left[EspError, T](MigratorCommunicationError(error)))
      }
    }
  }

  protected def request(path: String, method: HttpMethod, request: MessageEntity): Future[HttpResponse]

  override def compare(localProcess: DisplayableProcess)(implicit ec: ExecutionContext) : Future[Either[EspError, List[Difference]]] = {
    val id = localProcess.id

    (for {
      //TODO: move urls to some constants...
      process <- EitherT(invoke[ProcessDetails](s"processes/$id", HttpMethods.GET))
      compared <- EitherT.fromEither[Future](compareProcess(id, localProcess)(process))
    } yield compared).value

  }

  override def migrate(localProcess: DisplayableProcess)(implicit ec: ExecutionContext, loggedUser: LoggedUser) : Future[Either[EspError, Unit]]= {

    val comment = s"Process migrated from $environmentId by ${loggedUser.id}"
    (for {
      processToValidate <- EitherT.right(Marshal(localProcess).to[MessageEntity])
      validation <- EitherT(invoke[ValidationResult]("processValidation", HttpMethods.POST, processToValidate))
      _ <- EitherT.fromEither[Future](if (validation.errors != ValidationErrors.success) Left[EspError, Unit](MigratorValidationError(validation.errors)) else Right(()))
      processToSave <- EitherT.right(Marshal(ProcessToSave(localProcess, comment)).to[MessageEntity])
      result <- EitherT(invoke[ValidationResult](s"processes/${localProcess.id}", HttpMethods.PUT, processToSave))
    } yield ()).value
  }

  private def compareProcess(id: String, localProcess: DisplayableProcess)(remoteProcessDetails: ProcessDetails) : Either[EspError, List[Difference]] = remoteProcessDetails.json match {
    case Some(remoteProcess) => Right(ProcessComparator.compare(localProcess, remoteProcess))
    case None => Left(InvalidProcessTypeError(id))
  }

}
