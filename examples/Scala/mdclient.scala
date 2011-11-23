import org.zeromq.ZMsg
import util.control.Breaks._

/**
 * Majordomo Protocol client example. Uses the mdcli API to hide all MDP aspects
 * 
 * @author Josh Rotenberg <joshrotenberg@gmail.com>
 * 
 */
object mdclient {

  def main(args: Array[String]) = {

    var verbose = false
    if(args.length > 0 && args(0).matches("-v")) {
      verbose = true
    }

    val clientSession:mdcliapi = new mdcliapi("tcp://localhost:5555", verbose)

    var done = 0
    for(i <- 1 to 100) {
      var request:ZMsg = new ZMsg
      request.addString("Hello world")
      var reply = clientSession.send("echo", request)
      if(reply != null) {
	reply.destroy
	done += 1
      }
      else 
	break
      
    }
    printf("%d requests/replies processed\n", done)
    clientSession.destroy
  }

}
