package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor }
import kvstore.Arbiter._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

/**
  * The primary replica (root node) is will be resposible for replicating all changes
  * to a set of of secondary nodes (secondary nodes)
  *
  * KEY ASSUMPTIONS
  * The primary replica is the only one that can handle Insertions and Removals
  * The primary replica is the only one that can replicate its current state to secondary replicas
  * Both primary and secondary replicas can handle READ EVENTS
  * Secondary replicas can provide an outdated result for READ EVENTS
  *
  * OTHER ASSUMPTIONS
  * Updates are only ppossible on a dedicated node ==> ROOT
  * The root DOES NOT FAIL
  * Membership is handled reliably by the Arbiter
  * No incoming requests need to be rejected, because there is a low update rate.
  *
  */
object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]


  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case _ =>
  }

}

