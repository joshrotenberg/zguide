/**
 * (c) 2011 Josh Rotenberg
 *
 * This file is part of ZGuide
 *
 * ZGuide is free software; you can redistribute it and/or modify it under
 * the terms of the Lesser GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * ZGuide is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Lesser GNU General Public License for more details.
 *
 * You should have received a copy of the Lesser GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import org.zeromq.{ZContext, ZFrame, ZMQ, ZMsg}
import java.util.{ArrayDeque, Deque}
import scala.collection.immutable.HashMap
import util.control.Breaks._

/**
 *  Majordomo Protocol broker
 *  A minimal implementation of http://rfc.zeromq.org/spec:7 and spec:8
 *
 *  @author Josh Rotenberg <joshrotenberg@gmail.com>
 */


class mdbroker {

  private val ctx:ZContext = new ZContext // Our context
  private var socket:ZMQ.Socket = ctx.createSocket(ZMQ.ROUTER)
  private var heartbeatAt = System.currentTimeMillis() + 
    mdbroker.HEARTBEAT_INTERVAL //
  private var services = HashMap[String, Service]()
  private var workers = HashMap[String, Worker]()
  private val waiting = new ArrayDeque[Worker]()

  private var verbose = false
  class Service(var name:String) {
    
    val requests = new ArrayDeque[ZMsg]
    val waiting = new ArrayDeque[Worker]
  }

  class Worker(var identity:String, var address:ZFrame) {
    
    var service:Service = null
    var expiry = System.currentTimeMillis + mdbroker.HEARTBEAT_INTERVAL * 
    mdbroker.HEARTBEAT_LIVENESS
  }

  def mediate() = {
    breakable {
      while(!Thread.currentThread().isInterrupted) {
	var items = ctx.getContext.poller
	items.register(socket, ZMQ.Poller.POLLIN)
	if(items.poll(mdbroker.HEARTBEAT_INTERVAL * 1000) == -1)
	  break
	if(items.pollin(0)) {
	  var msg = ZMsg.recvMsg(socket)
	  if(msg == null)
	    break
	  if(verbose) {
	    println("I: received message:")
	    msg.dump(System.out)
	  }

	  val sender = msg.pop
	  val empty = msg.pop
	  val header = msg.pop

	  if(C_CLIENT.frameEquals(header)) {
	    processClient(sender, msg)
	  } 
	  else if(W_WORKER.frameEquals(header)) {
	    processWorker(sender, msg)
	  }
	  else {
	    println("E: invalid message:")
	    msg.dump(System.out)
	    msg.destroy
	  }
	  
	  sender.destroy
	  empty.destroy
	  header.destroy

	}
	purgeWorkers
	sendHeartbeats
      } // while
    } // breakable
    destroy
  }

  def destroy() = {
    workers.foreach ( (wrkr) => deleteWorker(wrkr._2, true) )
    ctx.destroy
  }

  def processClient(sender:ZFrame,  msg:ZMsg) = {
    assert(msg.size >= 2)
    val serviceFrame:ZFrame = msg.pop
    msg.wrap(sender.duplicate)
    if(serviceFrame.toString.startsWith(mdbroker.INTERNAL_SERVICE_PREFIX))
      serviceInternal(serviceFrame, msg)
    else
      dispatch(requireService(serviceFrame), msg)

    serviceFrame.destroy
  }

  def processWorker(sender:ZFrame, msg:ZMsg) = {
    assert(msg.size >= 1)
    val command = msg.pop
    val workerReady = workers.contains(sender.strhex)
    val worker = requireWorker(sender)

    if(W_READY.frameEquals(command)) {
      if(workerReady || sender.toString.startsWith(mdbroker.INTERNAL_SERVICE_PREFIX)) {
	deleteWorker(worker, true)
      }
      else {
	val serviceFrame = msg.pop
	worker.service = requireService(serviceFrame)
	workerWaiting(worker)
	serviceFrame.destroy
      }
    } else if(W_REPLY.frameEquals(command)) {
      if(workerReady) {
	val client = msg.unwrap
	msg.addFirst(worker.service.name)
	msg.addFirst(C_CLIENT.newFrame)
	msg.wrap(client)
	msg.send(socket)
	workerWaiting(worker)
      }
      else {
	deleteWorker(worker, true)
      }
    } else if(W_HEARTBEAT.frameEquals(command)) {
      if(workerReady) {
	worker.expiry = System.currentTimeMillis + mdbroker.HEARTBEAT_EXPIRY
      }
      else {
	deleteWorker(worker, true)
      }
    } else if(W_DISCONNECT.frameEquals(command)) {
      deleteWorker(worker, false)
    } else {
      println("E: invalid message:")
      msg.dump(System.out)
    }
    msg.destroy
  }


  private def deleteWorker(worker:Worker, disconnect:Boolean) = {
    assert(worker != null)
    if(disconnect) {
      sendToWorker(worker, W_DISCONNECT, null, null)
    }
    if(worker.service != null)
      worker.service.waiting.remove(worker)

    workers -= worker.identity
    worker.address.destroy
  }

  private def requireWorker(address:ZFrame):Worker = {
    assert(address != null)
    val identity = address.strhex
    var worker = workers.get(identity) match {
      case None => {
	var w:Worker = new Worker(identity, address.duplicate)
	workers += identity -> w
	if(verbose) 
	  printf("I: registering new worker: %s\n", identity)
	w
      }
      case Some(x) => x
    }
    worker
  }

  private def requireService(serviceFrame:ZFrame) = {
    assert(serviceFrame != null)
    val name = serviceFrame.toString
    var service = services.get(name) match {
      case None => {
        var s:Service = new Service(name)
	services += name -> s
	s
      }
      case Some(x) => x
    }
    service
  }
  
  private def bind(endPoint:String) = {
    socket.bind(endPoint)
    printf("I: MDP broker/0.1.1 is active at %s\n", endPoint)
  }

  private def serviceInternal(serviceFrame:ZFrame, msg:ZMsg) = {
    var returnCode = "501"
    if("mmi.service".equals(serviceFrame.toString)) {
      val name = msg.peekLast.toString
      returnCode = services.contains(name) match {
	case true => "200"
	case false => "400"
      }
    }
    msg.peekLast.reset(returnCode.getBytes)
    val client = msg.unwrap
    msg.addFirst(serviceFrame.duplicate)
    msg.addFirst(C_CLIENT.newFrame)
    msg.wrap(client)
    msg.send(socket)
  }

  def sendHeartbeats() = {
    synchronized {
      if(System.currentTimeMillis >= heartbeatAt) {
	waiting.toArray(Array[Worker]()).foreach( worker => sendToWorker(worker, W_HEARTBEAT, null, null))
      }
      heartbeatAt = System.currentTimeMillis + mdbroker.HEARTBEAT_INTERVAL
    }
  }

  def purgeWorkers() = {
    synchronized {
      // XXX
      waiting.toArray(Array[Worker]()).foreach( worker =>
	if(worker.expiry < System.currentTimeMillis) {
	  printf("I: deleting expired worker: %s\n", worker.identity)
	  deleteWorker(waiting.poll, false)
	})
    }
  }

  def workerWaiting(worker:Worker) = {
    synchronized {
      waiting.addLast(worker)
      worker.service.waiting.addLast(worker)
      worker.expiry = System.currentTimeMillis + mdbroker.HEARTBEAT_EXPIRY
      dispatch(worker.service, null)
    }
  }

  def dispatch(service:Service, msg:ZMsg) = {
    assert(service != null)
    if(msg != null)
      service.requests.offerLast(msg)
    purgeWorkers
    while(!service.waiting.isEmpty && !service.requests.isEmpty) {
      // XXX
      var msg = service.requests.pop
      var worker = service.waiting.pop
      waiting.remove(worker)
      sendToWorker(worker, W_REQUEST, null, msg)
      msg.destroy
    }
  }
  
  def sendToWorker(worker:Worker, command:MDP, option:String, msgp:ZMsg) = {
    var _m:ZMsg = msgp match {
      case null => new ZMsg
      case _ => msgp.duplicate
    }
    if(option != null)
      _m.addFirst(new ZFrame(option))

    _m.addFirst(command.newFrame)
    _m.addFirst(W_WORKER.newFrame)

    _m.wrap(worker.address.duplicate)
    if(verbose) {
      printf("I: sending %s to worker\n", command)
      _m.dump(System.out)
    }
    _m.send(socket)
  }
}

object mdbroker {

  val INTERNAL_SERVICE_PREFIX = "mmi."
  val HEARTBEAT_LIVENESS = 3
  val HEARTBEAT_INTERVAL = 2500
  val HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
  
  def main(args: Array[String]) = {
    val broker = new mdbroker()
    broker.bind("tcp://*:5555")
    broker.mediate
  }
}
