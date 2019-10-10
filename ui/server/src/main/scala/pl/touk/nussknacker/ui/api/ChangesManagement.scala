package pl.touk.nussknacker.ui.api

import pl.touk.nussknacker.engine.api.process.ProcessName
import pl.touk.nussknacker.engine.util.loader.ScalaServiceLoader
import pl.touk.nussknacker.ui.process.deployment.DeployInfo
import pl.touk.nussknacker.ui.process.repository.FetchingProcessRepository

import scala.concurrent.{ExecutionContext, Future}

trait ChangesManagement {
  def onArchived(repository: FetchingProcessRepository)(processName: ProcessName)(implicit ec: ExecutionContext): Future[Unit]
  def onCategoryChanged(repository: FetchingProcessRepository)(processName: ProcessName, category: String)(implicit ec: ExecutionContext): Future[Unit]
  def onDeleted(repository: FetchingProcessRepository)(processName: ProcessName)(implicit ec: ExecutionContext): Future[Unit]
  def onDeployed(repository: FetchingProcessRepository)(processName: ProcessName, deployInfo: DeployInfo)(implicit ec: ExecutionContext): Future[Unit]
  def onRenamed(repository: FetchingProcessRepository)(oldName: ProcessName, newName: ProcessName)(implicit ec: ExecutionContext): Future[Unit]
  def onSaved(repository: FetchingProcessRepository)(processName: ProcessName)(implicit ec: ExecutionContext): Future[Unit]
  def onUnarchived(repository: FetchingProcessRepository)(processName: ProcessName)(implicit ec: ExecutionContext): Future[Unit]
}

object ChangesManagement {
  def noop: ChangesManagement = aggregate()

  def serviceLoader(classLoader: ClassLoader): ChangesManagement = aggregate(ScalaServiceLoader.load[ChangesManagement](classLoader): _*)

  def aggregate(changes: ChangesManagement*): ChangesManagement = new ChangesManagement {
    override def onArchived(repository: FetchingProcessRepository)(processName: ProcessName)(implicit ec: ExecutionContext): Future[Unit] = {
      changes.foldLeft(Future.successful(())) { case (acc, change) =>
        acc.flatMap(_ => change.onArchived(repository)(processName))
      }
    }

    override def onCategoryChanged(repository: FetchingProcessRepository)(processName: ProcessName, category: String)(implicit ec: ExecutionContext): Future[Unit] = {
      changes.foldLeft(Future.successful(())) { case (acc, change) =>
        acc.flatMap(_ => change.onCategoryChanged(repository)(processName, category))
      }
    }

    override def onDeleted(repository: FetchingProcessRepository)(processName: ProcessName)(implicit ec: ExecutionContext): Future[Unit] = {
      changes.foldLeft(Future.successful(())) { case (acc, change) =>
        acc.flatMap(_ => change.onDeleted(repository)(processName))
      }
    }

    override def onDeployed(repository: FetchingProcessRepository)(processName: ProcessName, deployInfo: DeployInfo)(implicit ec: ExecutionContext): Future[Unit] = {
      changes.foldLeft(Future.successful(())) { case (acc, change) =>
        acc.flatMap(_ => change.onDeployed(repository)(processName, deployInfo))
      }
    }

    override def onRenamed(repository: FetchingProcessRepository)(oldName: ProcessName, newName: ProcessName)(implicit ec: ExecutionContext): Future[Unit] = {
      changes.foldLeft(Future.successful(())) { case (acc, change) =>
        acc.flatMap(_ => change.onRenamed(repository)(oldName, newName))
      }
    }

    override def onSaved(repository: FetchingProcessRepository)(processName: ProcessName)(implicit ec: ExecutionContext): Future[Unit] = {
      changes.foldLeft(Future.successful(())) { case (acc, change) =>
        acc.flatMap(_ => change.onSaved(repository)(processName))
      }
    }

    override def onUnarchived(repository: FetchingProcessRepository)(processName: ProcessName)(implicit ec: ExecutionContext): Future[Unit] = {
      changes.foldLeft(Future.successful(())) { case (acc, change) =>
        acc.flatMap(_ => change.onUnarchived(repository)(processName))
      }
    }
  }
}