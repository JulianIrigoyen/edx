package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.collection.IterableOnce.iterableOnceExtensionMethods

/**
  * //nvaa
  * GOAL  implement a distributed, replicated storage of key-value pairs
  *
  * The primary replica (root node) will be resposible for replicating all changes
  * to a set of of secondary replicas (secondary nodes)
  *
  * KEY ASSUMPTIONS
  * The primary replica is the only one that can handle Insertions and Removals
  * The primary replica is the only one that can replicate its current state to secondary replicas
  * Both primary and secondary replicas can handle READ EVENTS
  * Secondary replicas can provide an outdated result for READ EVENTS
  *
  * OTHER ASSUMPTIONS
  * Updates are only ppossible on a dedicated node ==> ROOT
  * The root DOES NOT FAIL (error kernel pattern)
  * Membership is handled reliably by the Arbiter
  * No incoming requests need to be rejected, because there is a low update rate.
  * When rejecting an update, the store is left in a possibly incosistent state
  *   which may require a subsequent ucceeding wite to the same key value pair
  *
  */
object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  /** Instructs Pr to insert key,value pair into the storage and replicate it to srs */
  case class Insert(key: String, value: String, id: Long) extends Operation
  /** Instructs Pr to remove the key and corresponding value from the storage and then remove it from srs */
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  override def preStart(): Unit = {
    arbiter ! Join
  }
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var _replicatorId = 0L

  var expectedSeq = 0L


  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {

    case Insert(key, value, id) => kv.get(key) match {
        case Some(_) =>
          sender() ! OperationAck(id)
        case None =>
          kv += key -> value
          sender() ! OperationAck(id)
      }

    case Remove(key, id) => kv.get(key) match {
        case Some(_) =>
          kv -= key
          sender() ! OperationAck(id)
        case None =>
          sender() ! OperationAck(id)
      }

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) =>
      val allTheReplicas: Set[ActorRef] = replicas - self
      val alreadyRunningReplicas: Set[ActorRef] = secondaries.keySet

      val newReplicas = allTheReplicas -- alreadyRunningReplicas
      newReplicas foreach { replica =>
        val replicator = createReplicator(replica)
        replicators += replicator
        replicate(replicator)
        secondaries += replica -> replicator
      }


    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Insert(_, _, id) => sender() ! OperationFailed(id)
    case Remove(_, id) => sender() ! OperationFailed(id)

    case snapshot @ Snapshot(key, valueOption, seq) =>
      log.info(s"{} received {}, sequence number {} ", self, snapshot, seq)
      seq match {
      case _ if seq == expectedSeq =>
        valueOption match {
          case Some(value) => kv += key -> value
          case None => kv -= key
        }
        sender() ! SnapshotAck(key, seq)
        acknowledgeThatStateWasUpdated()
      case _ if seq > expectedSeq => ()
      case _ if seq < expectedSeq =>
        sender() ! SnapshotAck(key, seq)

    }
    case Snapshot(_,_,_) => log.info(s"entro por akka")
    case Replicated(key, id) => log.info(s"entro por akka")



    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)



    case _ =>
      log.info("sale por aca")
  }

  def createReplicator(replica: ActorRef): ActorRef = {
    val newReplicatorid = nextReplicatorId()
    context.actorOf(Replicator.props(replica), s"replicator-$newReplicatorid")
  }

  def nextReplicatorId() = {
    val newId = _replicatorId
    _replicatorId += 1
    newId
  }

  def acknowledgeThatStateWasUpdated() = {
    expectedSeq += 1
  }
  def replicate(replicator: ActorRef): Unit = {
    for {
      key <- kv.keys
      value <- kv.values
    } yield replicator ! Replicator.Replicate(key, Some(value), 222)
  }

}

