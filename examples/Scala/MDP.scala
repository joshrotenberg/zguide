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
import org.zeromq.ZFrame

/**
 * Majordomo Protocol definitions, Scala version
 * 
 * @author Josh Rotenberg <joshrotenberg@gmail.com>
 */

sealed trait MDP { 
  def value:String
  def newFrame():ZFrame = {
    new ZFrame(this.value.getBytes) 
  }

  def frameEquals(frame:ZFrame):Boolean = {
    this.value.getBytes.sameElements(frame.getData())
  }
}

/**
 * This is the version of MDP/Client we implement
 */
case object C_CLIENT extends MDP { val value = "MDPC01" }

/**
 * This is the version of MDP/Worker we implement
 */
case object W_WORKER extends MDP { val value = "MDPW01" }

// MDP/Server commands, as strings
case object W_READY extends MDP { val value = "\001" }
case object W_REQUEST extends MDP { val value = "\002" }
case object W_REPLY extends MDP { val value = "\003" }
case object W_HEARTBEAT extends MDP { val value = "\004" }
case object W_DISCONNECT extends MDP { val value = "\005" }

object Foo {
  def main(args: Array[String]) = {
    println(C_CLIENT.newFrame)
    println(C_CLIENT.frameEquals(C_CLIENT.newFrame))
    println(C_CLIENT.frameEquals(W_WORKER.newFrame))
  }
}

