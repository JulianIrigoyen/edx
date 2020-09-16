/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case operation: Operation =>
      root ! operation

    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case operation: Operation => pendingQueue :+= operation
    case CopyFinished =>
      root = newRoot
      context.unbecome()
      for (pendingOp <- pendingQueue) newRoot ! pendingOp
      pendingQueue = Queue.empty[Operation]
      log.info("Finished collecting garbage")
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) =
    Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor
  with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case cmd @ Insert(requester, id, elemToInsert) =>
      if (elemToInsert == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else {
        val next = nextPostion(elemToInsert)
        if(subtrees.contains(next)) subtrees(next) ! cmd
        else {
          subtrees += next -> createNewNode(elemToInsert)
          requester ! OperationFinished(id)
          log.info(s"INSERTED $elemToInsert to the $next of $elem")
        }
      }

    case cmd @ Remove(requester, id, elemToRemove) =>
      if(elemToRemove == elem) {
        removed = true
        requester ! OperationFinished(id)
        log.info(s"REMOVED $elemToRemove")
      } else {
        val next = nextPostion(elemToRemove)
        if(subtrees.contains(next)) subtrees(next) ! cmd
        else requester ! OperationFinished(id)
      }

    case cmd @ Contains(requester, id, elemToFind) =>
      if(elemToFind == elem && !removed) requester ! ContainsResult(id, true)
      else {
        val next = nextPostion(elemToFind)
        if(subtrees.contains(next)) subtrees(next) ! cmd
        else requester ! ContainsResult(id, false)
      }

    case msg @ CopyTo(newTree) =>
      if(subtrees.isEmpty && removed) {
        context.parent ! CopyFinished
        self ! PoisonPill
      } else {
        if (!removed) newTree ! Insert(self, elem * 10, elem)
        val expected = subtrees.values.toSet
        for(node <- subtrees.values) node ! msg //subtrees.foreach { _ ! msg }
        context.become(copying(expected, removed))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {

    case OperationFinished(_) =>
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        self ! PoisonPill
      } else {
        context.become(copying(expected, true))
      }

    case CopyFinished =>
      val remaining = expected - sender()
      if(remaining.isEmpty && insertConfirmed){
        context.parent ! CopyFinished
        self ! PoisonPill
      } else context.become(copying(remaining, insertConfirmed))
  }

  def createNewNode(elemToInsert: Int): ActorRef =
    context.actorOf(props(elemToInsert, initiallyRemoved = false), s"node-of-$elemToInsert")

  val nextPostion: Int => Position = (incomingElem: Int) => if(incomingElem > elem) Right else Left

}
