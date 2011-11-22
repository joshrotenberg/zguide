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
 * Majordomo Protocol Client API, Scala version Implements the MDP/Worker
 * spec at http://rfc.zeromq.org/spec:7.
 * 
 * @author Josh Rotenberg <joshrotenberg@gmail.com>
 */
class mdcliapi(broker:String, verbose:Boolean) {

  private var ctx:ZContext = new ZContext()
  private var client:ZMQ.Socket = null

  var timeout:Long = 2500
  var retries:Int = 3
  reconnectToBroker

  /**
   * Connect or reconnect to broker
   */
  def reconnectToBroker() = {
    if(client != null) {
      ctx.destroySocket(client)
    }
    client = ctx.createSocket(ZMQ.REQ)
    client.connect(broker)
    if(verbose) 
      printf("I: connecting to broker at %s...\n", broker);
    
  }

  /**
   * Send request to broker and get reply by hook or crook Takes ownership of
   * request message and destroys it when sent. Returns the reply message or
   * NULL if there was no reply.
   * 
   * @param service
   * @param request
   * @return
   */
  def send(service:String, request:ZMsg):ZMsg = {

    request.push(new ZFrame(service))
    request.push(C_CLIENT.newFrame)
    if(verbose) {
      printf("I: send request to '%s' service: \n", service);
      request.dump(System.out)
    }

    var reply:ZMsg = null
    var retriesLeft = retries

    breakable {
      while(retriesLeft > 0 &&  !Thread.currentThread().isInterrupted()) {
      
	request.duplicate().send(client)

        // Poll socket for a reply, with timeout
	var items = ctx.getContext().poller()
	items.register(client, ZMQ.Poller.POLLIN)
	
	if(items.poll(timeout * 1000) == -1) {
	  break // Interrupted
	}

	if(items.pollin(0)) {
	  val msg = ZMsg.recvMsg(client)
	  if(verbose) {
	    println("I: received reply: \n")
	    msg.dump(System.out)
	  }
          // Don't try to handle errors, just assert noisily
	  assert(msg.size >= 3)

	  val header = msg.pop
	  assert(C_CLIENT.value.equals(header.toString))
	  header.destroy

	  val replyService = msg.pop
	  assert(service.equals(replyService.toString))
	  replyService.destroy
	  
	  reply = msg
	  break
	} else {
	  items.unregister(client)
	  retriesLeft -= 1
	  if(retriesLeft == 0) {
	    println("W: permanent error, abandoning");
	    break
	  }
          println("W: no reply, reconnecting...");
	  reconnectToBroker
	}
      }	
    }

    request.destroy
    reply
  }
  

  def destroy = ctx.destroy
}

