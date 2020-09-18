package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  case class RetryUntilAcknowledged(key: String, valueOption: Option[String], seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var pendingAcknowledgements = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  /** sequence number (seq) to enforce ordering between the updates. Updates for a given secondary replica must be processed in contiguous ascending sequence number order;
    * this ensures that updates for every single key are applied in the correct order.  */
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case updateOperation @ Replicate(key, valueOption ,id) =>
      val updateNumber = nextSeq()
      log.info("Processing replication request number {} for replica {} for key: {}, value: {}", updateNumber, replica, key, valueOption)
      replica ! Snapshot(key, valueOption, updateNumber)
      pendingAcknowledgements += (updateNumber -> (sender(), updateOperation))

      context.system.scheduler.scheduleOnce(10.milliseconds) {
        self ! RetryUntilAcknowledged(key, valueOption, updateNumber)
      }


    case RetryUntilAcknowledged(key, valueOption, seq) if pendingAcknowledgements contains seq =>
      log.info(s"retrying snpashot")
      replica  ! Snapshot(key, valueOption, seq)
      context.system.scheduler.scheduleOnce(10.milliseconds) {
        self ! RetryUntilAcknowledged(key, valueOption, seq)
      }

    case SnapshotAck(key,seq) if pendingAcknowledgements contains seq =>
      for((replica, operation) <- pendingAcknowledgements.get(seq))
        replica ! Replicated(key, operation.id)
      pendingAcknowledgements -= seq


    case _ =>

  }

  def handleSnapchotAck(msg: SnapshotAck) = {
    val correspondingAcknowledgement = pendingAcknowledgements(msg.seq)
    val correspondingReplica = correspondingAcknowledgement._1
    val correspondingOperation = correspondingAcknowledgement._2

    correspondingReplica ! Replicated(msg.key, correspondingOperation.id)

    pendingAcknowledgements -= msg.seq
  }

  def retryUntilAcknowledged(ack: SnapshotAck) = {

  }

}
