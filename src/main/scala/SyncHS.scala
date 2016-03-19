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
  case class ReplyMsg(id: Int, dir: Boolean)  extends Msg // id is the dest node id
  case class RejectMsg(id: Int, dir: Boolean) extends  Msg  // id represents dest node id

  case class LeaderMsg(id: Int, dir: Boolean) extends Msg   // id is the id of the leader

  // when a node knows its the leader ... broadcasts this to the ring
  var leaderElected = new AtomicBoolean(false)  // initially false used to terminate LE alg

  var prevTLeaderCount = new AtomicInteger(0)
  var presentTLeaderCount = new AtomicInteger(0)
  var doneTLeaderThreads = new AtomicInteger(0)
  var replyDoneThreads = new AtomicInteger(0)
  var numThrds = 0

  //create a ConcurrentHashMap with initialCapacity = concurrencyLevel = numofThreads   ... at a given time all threads might be accessing this map
  var thrdMsgsMap: ConcurrentHashMap[Int, ArrayBlockQ[Msg]] = null.asInstanceOf[ConcurrentHashMap[Int, ArrayBlockQ[Msg]]]
  var phaseDone: ConcurrentHashMap[Int, Boolean] = null.asInstanceOf[ConcurrentHashMap[Int, Boolean]]

  case class Node(id: Int,
                  left: Int,
                  right: Int,
                  cyclicBarrier: CyclicBarrier
                 ) extends Runnable{
    var isInRace: Boolean = true
    var phaseDone: Boolean = false
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

          thrdMsgsMap.get(left).put(ProbeMsg(id, phase, scala.math.pow(2d, phase).toInt, false))
          thrdMsgsMap.get(right).put(ProbeMsg(id, phase, scala.math.pow(2d, phase).toInt, true))
          var replyCount = 0
          var rejectCount = 0
          var totalMsgCount = 0
          phaseDone = false

          while (!phaseDone) {
            ////println(s"$id am still in race ")
            // below call blocks until reply to above messages are in the queue for this thread
            val msg = thrdMsgsMap.get(id).poll(10L, TimeUnit.NANOSECONDS)
            if(msg == null) {////println(s"---------------$id timed out waiting for msg")
            }
            else totalMsgCount += 1
            (msg: AnyRef) match {
              case p: ProbeMsg =>
                if (id == p.id) {
                  println(s"$id **** **** I am the LEADER!  YEAAAAAAH **** ***** ")
                  leaderElected.getAndSet(true)
                  thrdMsgsMap.get(right).put(LeaderMsg(id, true ))
                  thrdMsgsMap.get(left).put(LeaderMsg(id, false))
                  //println(s"LEADER -> id $id sent messages to my neighbours")
                  phaseDone = true
                  isInRace = false
                  //println("LEADER -> id Waiting at barrier for everyone")
                  cyclicBarrier.await()
                }
                else if (id < p.id && p.stepCount == 1) {
                  //println(s"$id sending REPLY to $p.id")
                  if (p.dir == true) thrdMsgsMap.get(this.left).put(ReplyMsg(p.id, !p.dir))
                  else thrdMsgsMap.get(this.right).put(ReplyMsg(p.id, !p.dir))
                }
                else if (id > p.id ) {
                  //println(s"$id sending reject to $p.id")
                  if (p.dir) thrdMsgsMap.get(this.left).put(RejectMsg(p.id, !p.dir))
                  else thrdMsgsMap.get(this.right).put(RejectMsg(p.id, !p.dir))
                }
                else {
                  if (p.dir == true) thrdMsgsMap.get(this.right).put(p.copy(stepCount = p.stepCount - 1))
                  else thrdMsgsMap.get(this.left).put(p.copy(stepCount = p.stepCount - 1))
                }
              case rp: ReplyMsg =>
                if (id == rp.id) {
                  //println(s"$id Got reply ")
                  replyCount = replyCount + 1
                  if((replyCount == 2) || (rejectCount == 1 && replyCount == 1))  {
                    println(s"$id TempLeader at $phase")
                    println(s" $id incrementing count of replyDoneThrds")
                    replyDoneThreads.getAndIncrement()
                    haveSetReplyDoneThrds = true
                  }
                }
                else {
                  if (rp.dir == true) thrdMsgsMap.get(this.right).put(rp.copy())
                  else thrdMsgsMap.get(this.left).put(rp.copy())
                }
              case rj: RejectMsg =>
                if (id == rj.id) {
                  rejectCount = rejectCount + 1
                  if ((rejectCount == 2) || (rejectCount == 1 && replyCount == 1)) {
                    //println(s"$id incrementing rejectDone")
                    replyDoneThreads.getAndIncrement()
                    haveSetReplyDoneThrds = true
                  }
                }
                else {
                  if (rj.dir == true) thrdMsgsMap.get(this.right).put(rj.copy())
                  else thrdMsgsMap.get(this.left).put(rj.copy())
                }
              case lm :LeaderMsg =>
                leader = lm.id
                phaseDone = true
                isInRace = false
                println(s"$id my leader -> $leader")
                if (lm.dir == true) thrdMsgsMap.get(right).put(lm.copy()) else thrdMsgsMap.get(left).put(lm.copy())
                cyclicBarrier.await()
              case null => ; // do nothing
              case _ => //println("Something wrong going in here  Match didnot find any matches when inRace")
            }
            if (((rejectCount == 2 | replyCount == 2 | (replyCount + rejectCount == 2) ) |(phaseDone) )&& replyDoneThreads.get() == prevTLeaderCount.get() ){
              if(rejectCount >= 1 ) {
                isInRace = false
                //println(s"$id decrementing tleadercount")
                presentTLeaderCount.getAndDecrement()   // remove this from the present count for active temp leaders
              }
              phaseDone = true
              //println(s"$id Waiting at barrier")
              cyclicBarrier.await()
              //println(s"$id released")
              replyCount = 0 ; totalMsgCount = 0 // reset counters
              //phase += 1
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
            if(doneTLeaderThreads.get() != prevTLeaderCount.get())
              msg = thrdMsgsMap.get(id).poll(1000L, TimeUnit.NANOSECONDS)
            (msg : AnyRef) match {
              case p: ProbeMsg =>
                if (id < p.id && p.stepCount == 1) {
                  //println(s"$id sending REPLY to $p.id")
                  if (p.dir == true) thrdMsgsMap.get(this.left).put(ReplyMsg(p.id, !p.dir))
                  else thrdMsgsMap.get(this.right).put(ReplyMsg(p.id, !p.dir))
                }
                else if (id > p.id ) {
                  //println(s"$id sending reject to $p.id")
                  if (p.dir == true) thrdMsgsMap.get(this.left).put(RejectMsg(p.id, !p.dir))
                  else thrdMsgsMap.get(this.right).put(RejectMsg(p.id, !p.dir))
                }
                else {
                  //println(s"$id forwarding $p ")
                  if (p.dir == true) thrdMsgsMap.get(this.right).put(p.copy(stepCount = p.stepCount - 1))
                  else thrdMsgsMap.get(this.left).put(p.copy(stepCount = p.stepCount - 1))
                }
              case r: ReplyMsg =>
                //println(s"$id forwarding $r ")
                if (r.dir == true)
                  thrdMsgsMap.get(right).put(r.copy())
                else
                  thrdMsgsMap.get(left).put(r.copy())
              case rj: RejectMsg =>
                //println(s"$id forwarding $rj ")
                if (rj.dir == true)
                  thrdMsgsMap.get(right).put(rj.copy())
                else
                  thrdMsgsMap.get(left).put(rj.copy())
              case lm: LeaderMsg =>
                leader = lm.id
                println(s"$id my leader $leader  SALUTE!")
                if (lm.dir == true) thrdMsgsMap.get(right).put(lm.copy()) else thrdMsgsMap.get(left).put(lm.copy())
                phaseDone = true
                cyclicBarrier.await()
              case null => // do nothing
              case _ => ////println("Something wrong", msg.toString())
            }
            /*if ((rejectCount == 2 | replyCount ==2 | (replyCount + rejectCount == 2) ) | (phaseDone | forwardCount ==0 ) & msgCount >= 2)  {
              phaseDone = true ; //println(s"Waiting $id") ; cyclicBarrier.await(); //println(s"$id released") ;//phase += 1
            }*/
            if (doneTLeaderThreads.get() == prevTLeaderCount.get()) {
              phaseDone = true
              cyclicBarrier.await()
              //println(s"$id released")
            }
          }
          phase = phase + 1
        }
      }
      //println(s"$id Exiting leader = $leader")
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

    var pos = -1
    for (id <-ids) thrdMsgsMap.put(id, new ArrayBlockQ[Msg](4))

    for (id <- ids) {
      pos += 1
      val left = if (pos == 0) ids(numThrds - 1) else ids(pos - 1)
      val right = if (pos == numThrds - 1) ids(0) else ids(pos + 1)
      ////println(left, right)
      new Thread(new Node(id, left, right, barrier), id.toString).start()
    }

    try
        while (!leaderElected.get()){

          if (barrier.getNumberWaiting == prevTLeaderCount.get()) {   // check if all the prevTLeaders are waiting ... that means they are done .. time to switch off the other threads
            doneTLeaderThreads.set(prevTLeaderCount.get())

            while (!(barrier.getNumberWaiting == numThrds)){Thread.sleep(10)}  // wait till the relay threads notice that TempLeaders are done and they do barrier.await

            doneTLeaderThreads.set(0)
            val now = presentTLeaderCount.get()
            prevTLeaderCount.set(now)
            replyDoneThreads.set(0)
            barrier.await()

          }
        }
    catch {
      case e: InterruptedException => e.printStackTrace()
      case e: BrokenBarrierException => e.printStackTrace()
    }
    if (leaderElected.get()) {
      do {
          Thread.sleep(10)
        } while (barrier.getNumberWaiting() != numThrds)
      System.exit(1)
    }
  }


  def parse(path: String, numThrds: Int): ArrayBuffer[Int] = {
    val ids: ArrayBuffer[Int] = ArrayBuffer()
    //println(path)
    try {
      val input= Source.fromFile(path).getLines().map(_.toInt).toSeq
      ids.insertAll(0,input)
      require(input.size == input.toSet.size, {
        println("UIDS are not unique! this algorithm assumes UIDS")
        input.map( id => (id,1)).groupBy(_._1).collect {
          case (id,count ) => if (count.size > 1) println(s"Duplicate : $id  count -> ", count.size )
        }
        System.exit(1)
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