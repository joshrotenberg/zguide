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
import util.control.Breaks._

/**
 * Majordomo Protocol Worker API, Scala version Implements the MDP/Worker
 * spec at http://rfc.zeromq.org/spec:7.
 * 
 * @author Josh Rotenberg <joshrotenberg@gmail.com>
 */

class mdwrkapi(broker:String, service:String, verbose:Boolean) {

  private val HEARTBEAT_LIVENESS = 3 // 3-5 is reasonable

  private var ctx:ZContext = new ZContext()

  private var worker:ZMQ.Socket = null // Socket to broker
  private var heartbeatAt:Long = 0 // When to send HEARTBEAT
  private var liveness:Int = 0 // How many attempts left
  private var heartbeat = 2500 // Heartbeat delay, msecs
  private var reconnect = 2500 // Reconnect delay, msecs

  private var expectReply = false // false only at start
  private var replyTo:ZFrame = null // Return address, if any
  
  var timeout:Long = 2500  
  
  reconnectToBroker
  /**
   * Send message to broker If no msg is provided, creates one internally
   * 
   * @param command
   * @param option
   * @param msg
   */

  def sendToBroker(command:MDP, option:String, msg:ZMsg) = {
    var _m:ZMsg = msg match {
      case null => new ZMsg
      case _ => msg.duplicate
    }
    
    if(option != null)
      _m.addFirst(new ZFrame(option))
    
    _m.addFirst(command.newFrame)
    _m.addFirst(W_WORKER.newFrame)
    _m.addFirst(new ZFrame(new Array[Byte](0)))

    if(verbose) {
      printf("I: sending %s to broker\n", command)
      _m.dump(System.out)
    }

    _m.send(worker)
  }

  /**
     * Connect or reconnect to broker
     */
  def reconnectToBroker() = {
    if (worker != null) {
      ctx.destroySocket(worker)
    }
    worker = ctx.createSocket(ZMQ.DEALER)
    worker.connect(broker)
    if (verbose)
      printf("I: connecting to broker at %s...\n", broker)
    
    // Register service with broker
    sendToBroker(W_READY, service, null)
    
    // If liveness hits zero, queue is considered disconnected
    liveness = HEARTBEAT_LIVENESS
    heartbeatAt = System.currentTimeMillis() + heartbeat

  }

  def receive(reply:ZMsg):ZMsg = {

    // Format and send the reply if we were provided one
    assert(reply != null || !expectReply)

    if(reply != null) {
      assert(replyTo != null)
      reply.wrap(replyTo)
      sendToBroker(W_REPLY, null, reply)
      reply.destroy
    }

    expectReply = true
    var rmsg:ZMsg = null
    breakable {
      while(!Thread.currentThread().isInterrupted) {
        // Poll socket for a reply, with timeout
	var items = ctx.getContext.poller
	items.register(worker, ZMQ.Poller.POLLIN)
	if(items.poll(timeout * 1000) == -1) {
	  break // Interrupted
	}
	if(items.pollin(0)) {
	  var msg = ZMsg.recvMsg(worker)
	  if(msg == null) {
	    break // Interrupted
	  }
	  if(verbose) {
	    println("I: received message from broker:")
	    msg.dump(System.out)
	  }

	  liveness = HEARTBEAT_LIVENESS
          // Don't try to handle errors, just assert noisily
	  assert (msg != null && msg.size >= 3)

	  val empty = msg.pop
	  assert(empty.getData.length == 0)
	  empty.destroy
	  
	  val header = msg.pop
	  assert(W_WORKER.frameEquals(header))
	  header.destroy

	  val command = msg.pop
	  if(W_REQUEST.frameEquals(command)) {
	    replyTo = msg.unwrap
	    command.destroy

	    // XXX here
	    //return msg
	    rmsg = msg
	    break
	  }
	  else if(W_HEARTBEAT.frameEquals(command)) {
	    // Do nothing for heartbeats
	  } 
	  else if(W_DISCONNECT.frameEquals(command)) {
	    reconnectToBroker
	  }
	  else {
	    println("E: invalid input message: ")
	    msg.dump(System.out)
	  }
	  command.destroy
	  msg.destroy
	} else {
	  liveness -= 1

	  if(liveness == 0) {
	    if(verbose) 
	      println("W: disconnected from broker - retrying...")
	    try {
	      Thread.sleep(reconnect)
	    } catch {
	      case e:InterruptedException => {
		Thread.currentThread.interrupt
		break
	      }
	    }
	    reconnectToBroker
	  }
	}
	
	if(System.currentTimeMillis > heartbeatAt) {
	  sendToBroker(W_HEARTBEAT, null, null)
	  heartbeatAt = System.currentTimeMillis + heartbeat
	}
      }
    }

    if(Thread.currentThread.isInterrupted)
      println("W: interrupt received, killing worker...")

    if(rmsg != null)
      return rmsg
    else
      return null
  }

  def destroy = ctx.destroy
}

