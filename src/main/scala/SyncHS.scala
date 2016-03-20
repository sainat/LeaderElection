/**
  * Created by Sainath on 2/10/2016.
  */

import java.io.FileNotFoundException
import java.util.concurrent.TimeUnit
import java.util.concurrent.{ArrayBlockingQueue => ArrayBlockQ, BrokenBarrierException, CyclicBarrier, ConcurrentHashMap}
import scala.io.Source

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic._


object SyncHS {
  /*
    takes two input arguments from command line
    n -> number of threads(nodes)  &
    input.dat location -> file containing all the node id's (non negative)
   */
  trait  Msg {
    val id: Int
    val dir: Boolean
  }
  case class ProbeMsg( id: Int,// source id
                       phase: Int, // the current phase
                       stepCount: Int, // steps from the origin <= 2^phase
                       dir: Boolean) extends Msg

  // direction of travel in the ring for this msg
  case class ReplyMsg(id: Int, dir: Boolean)  extends Msg // ACK in reply to probeMsg  if id is greatest in the 2 ^ n-1 0neighborhood .id is the dest node id
  case class RejectMsg(id: Int, dir: Boolean) extends  Msg  // NAK in reply to ProbeMsg  id represents dest node id

  case class LeaderMsg(id: Int, dir: Boolean) extends Msg   // id -> id of the leader
  // when a node knows its the leader ... broadcasts this to the ring
  val leaderElected = new AtomicBoolean(false)  // initially false used to terminate LE alg

  val prevTLeaderCount = new AtomicInteger(0)
  val presentTLeaderCount = new AtomicInteger(0)
  val doneTLeaderThreads = new AtomicInteger(0)
  val replyDoneThreads = new AtomicInteger(0)
  var numThrds = 0

  //create a ConcurrentHashMap with initialCapacity = concurrencyLevel = numofThreads   ... at a given time all threads might be accessing this map
  var thrdMsgsMap: ConcurrentHashMap[Int, ArrayBlockQ[Msg]] = null
  var phaseDone: ConcurrentHashMap[Int, Boolean] = null

  case class Node(id: Int,
                  left: Int,
                  right: Int,
                  cyclicBarrier: CyclicBarrier
                 ) extends Runnable {
    var isInRace: Boolean = true
    var phaseDone: Boolean = false
    var replyCount = 0
    var rejectCount = 0
    var leader: Int = Integer.MIN_VALUE
    var phase = 0

    var haveSetReplyDoneThrds = false


    def run() = {
      while (!leaderElected.get()) {
        haveSetReplyDoneThrds = false
        phaseDone = false
        /*  if     node is still in running for leader position, keep sending neighbours probe messages and reply to other probe  */
        while (isInRace) {
          // broadcast probe message to the neighbours and wait till we get replys for those
          sendProbes()
          phaseDone = false
          while (!phaseDone) {
            val msg = thrdMsgsMap.get(id).poll(10L, TimeUnit.NANOSECONDS) // call blocks until reply to above messages are in the queue for this thread
            handleMessage(msg)

            if ((( rejectCount == 2 | replyCount == 2 | replyCount + rejectCount == 2) | phaseDone)
               && replyDoneThreads.get() == prevTLeaderCount.get()
            ){
              if(rejectCount >= 1 ) {
                isInRace = false
                presentTLeaderCount.getAndDecrement()   // remove this node from the our present temporary leader count
              }
              phaseDone = true
              cyclicBarrier.await()
              replyCount = 0 ; // reset counters
            }
          }
          phase = phase + 1
        }
        /*else
        *     when not in race, keep relaying messages
        * */
        while(!isInRace & !leaderElected.get()){
          println(s"$id Relay Node Entering phase $phase")
          phaseDone = false
          while(!phaseDone){
            var msg:AnyRef = AnyRef
            if(doneTLeaderThreads.get() != prevTLeaderCount.get()) // try to relay messages until all the temporary leaders have finished (doneTLeaderThreads)
              msg = thrdMsgsMap.get(id).poll(1000L, TimeUnit.NANOSECONDS)

            (msg : AnyRef) match {
              case p: ProbeMsg => probeMsgHandler(p)
              case rp: ReplyMsg => forwardMsg(rp)
              case rj: RejectMsg => forwardMsg(rj)
              case lm: LeaderMsg =>
                leader = lm.id
                println(s"$id my leader $leader  SALUTE!")
                forwardMsg(lm)
                phaseDone = true
                cyclicBarrier.await()
              case null => // do nothing
              case _ => println("Something wrong", msg.toString)
            }
            if (doneTLeaderThreads.get() == prevTLeaderCount.get()) {
              phaseDone = true
              cyclicBarrier.await()
            }
          }
          phase = phase + 1
        }
      }
    }

    def leftNodeQ = thrdMsgsMap.get(left)
    def rightNodeQ = thrdMsgsMap.get(right)

    /*
    * sends probes in left/right neighborhood that travel maximum of  (2^round-1) hops
    */
    def sendProbes() = {
      leftNodeQ.put(ProbeMsg(id, phase, scala.math.pow(2d, phase).toInt, dir = false))
      rightNodeQ.put(ProbeMsg(id, phase, scala.math.pow(2d, phase).toInt, dir = true))
    }

    def replyMsgHandler(rp: ReplyMsg) = {
      if (id == rp.id) {
        replyCount = replyCount + 1
        if ((replyCount == 2) || (rejectCount == 1 && replyCount == 1)) {
          println(s"$id TempLeader at $phase")
          println(s" $id incrementing count of replyDoneThrds")
          replyDoneThreads.getAndIncrement()
          haveSetReplyDoneThrds = true
        }
      }
      else
        forwardMsg(rp)
    }

    def sendReply(destNodeID: Int, probeDir: Boolean) = {
      // will send reply(ok) left/right based on the original probeMessage's Dir
      if (probeDir) leftNodeQ.put(ReplyMsg(destNodeID, !probeDir))
      else rightNodeQ.put(ReplyMsg(destNodeID, probeDir))
    }
    def sendReject(destNodeID: Int, probeDir: Boolean) = {
      if(probeDir) leftNodeQ.put(RejectMsg(destNodeID, !probeDir))
      else rightNodeQ.put(RejectMsg(destNodeID, !probeDir))
    }
    def forwardMsg(msg: Msg) = {
      msg match{
        case p: ProbeMsg =>
          if (p.dir) rightNodeQ.put(p.copy(stepCount = p.stepCount - 1))
          else leftNodeQ.put(p.copy(stepCount = p.stepCount - 1))
        case x: Msg =>
          if (x.dir) rightNodeQ.put(x)
          else leftNodeQ.put(x)
      }
    }

    def handleMessage(msg: AnyRef) {
      msg match {
        case p: ProbeMsg => probeMsgHandler(p)
        case rp: ReplyMsg => replyMsgHandler(rp)
        case rj: RejectMsg => rejectMsgHandler(rj)
        case lm: LeaderMsg =>
          leader = lm.id
          phaseDone = true
          isInRace = false
          println(s"$id my leader -> $leader")
          if (lm.dir) rightNodeQ.put(lm.copy()) else leftNodeQ.put(lm.copy())
          cyclicBarrier.await()
        case null => ; // do nothing
        case _ => //println("Something wrong going in here  Match didnot find any matches when inRace")
      }
    }

    def declareMeLeader() = {
      println(s"$id **** **** I am the LEADER!  YEAAAAAAH **** ***** ")
      leaderElected.getAndSet(true)
      rightNodeQ.put(LeaderMsg(id, dir = true))
      leftNodeQ.put(LeaderMsg(id, dir = false))
      //println(s"LEADER -> id $id sent messages to my neighbours")
      phaseDone = true
      isInRace = false
    }

    def probeMsgHandler(p: ProbeMsg) = {
      if (id == p.id) {
        declareMeLeader()
        cyclicBarrier.await()
      }
      else if (id < p.id && p.stepCount == 1) // only the last node in 2 ^ (round -1) neighborhood can ack a Probe
        sendReply( p.id, p.dir)
      else if (id > p.id) // if id is smaller than my id immdediately send a reject
        sendReject(p.id, p.dir)
      else
        forwardMsg(p)
    }

    def rejectMsgHandler(rj: RejectMsg) = {
      if (id == rj.id) {
        rejectCount = rejectCount + 1
        if ((rejectCount == 2)
           || (rejectCount == 1 && replyCount == 1)
        ){
          replyDoneThreads.getAndIncrement()  // processing done for this round
          haveSetReplyDoneThrds = true
        }
      }
      else
        forwardMsg(rj)
    }
  }

  def startThreads(ids: ArrayBuffer[Int], barrier: CyclicBarrier) = {
    var pos = -1

    for (id <- ids) {
      pos += 1
      val left = if (pos == 0) ids(numThrds - 1) else ids(pos - 1)
      val right = if (pos == numThrds - 1) ids(0) else ids(pos + 1)

      new Thread(new Node(id, left, right, barrier), id.toString).start()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Intended usage: SyncHS <number of nodes>  <input.dat location>")
      System.exit(1)
    }
    numThrds = args(0).toInt

    prevTLeaderCount.getAndAdd(numThrds)  // initially same as presnt templeader which is equal to the total threads
    presentTLeaderCount.getAndAdd(numThrds)

    val barrier: CyclicBarrier = new CyclicBarrier(numThrds + 1)
    val ids = parse(args(1), numThrds)
    thrdMsgsMap = new ConcurrentHashMap[Int, ArrayBlockQ[Msg]](numThrds, numThrds)
    phaseDone = new ConcurrentHashMap[Int, Boolean](numThrds, numThrds)

    for (id <-ids) thrdMsgsMap.put(id, new ArrayBlockQ[Msg](4))

    startThreads(ids, barrier)

    try {
      while (!leaderElected.get()) {
        if (barrier.getNumberWaiting == prevTLeaderCount.get()) {
          // check if all the prevTLeaders are waiting ... that means they are done .. time to switch off the other threads
          doneTLeaderThreads.set(prevTLeaderCount.get())
          while (!(barrier.getNumberWaiting == numThrds)) {
            Thread.sleep(10)
          } // wait till the relay threads notice that TempLeaders are done and they do barrier.await

          doneTLeaderThreads.set(0)
          val now = presentTLeaderCount.get()
          prevTLeaderCount.set(now)
          replyDoneThreads.set(0)
          barrier.await()

        }
      }
    }
    catch {
      case e: InterruptedException => e.printStackTrace()
      case e: BrokenBarrierException => e.printStackTrace()
    }
    if (leaderElected.get()) {
      do {
        Thread.sleep(10)
      } while (barrier.getNumberWaiting != numThrds)
      System.exit(1)
    }
  }

  def parse(path: String, numThrds: Int): ArrayBuffer[Int] = {
    val ids: ArrayBuffer[Int] = ArrayBuffer()
    //println(path)
    try {
      val input= Source.fromFile(path)
        .getLines()
        .map(_.toInt)
        .toSeq

      ids.insertAll(0,input)

      require (input.size == input.toSet.size, {
        println ("UIDS are not unique! this algorithm assumes UIDS")
        input.map( id => (id,1))
          .groupBy(_._1)
          .collect {
            case (id,count ) => if (count.size > 1) println(s"Duplicate : $id  count -> ", count.size )
          }
        System.exit (1)
      })
    } catch{
      case e :FileNotFoundException => println(e.printStackTrace())
      case e :Exception => println(e.printStackTrace())
    }
    if (ids.size < 2) {
      System.err.println("Leader election is trivial in graphs with less than 2 nodes")
      System.exit(1)
    }
    ids
  }
}