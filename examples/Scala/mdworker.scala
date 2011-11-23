import org.zeromq.ZMsg
import util.control.Breaks._

object mdworker {
  
  def main(args: Array[String]) = {
    val workerSession = new mdwrkapi("tcp://localhost:5555", "echo", true)
    
    var reply:ZMsg = null
    while(!Thread.currentThread.isInterrupted) {
      var request = workerSession.receive(reply)
      if(request == null) 
	break
      reply = request
    }

    workerSession.destroy
  }
}
