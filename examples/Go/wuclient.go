/*
 *  Weather proxy listens to weather server which is constantly
 *  emitting weather data
 *  Binds SUB socket to tcp://*:5556
*/
package main

import (
  "fmt"
  "os"
  "strings"
  "strconv"
  zmq "github.com/alecthomas/gozmq"
)
func main() {
  context, _ := zmq.NewContext()
  socket, _ := context.NewSocket(zmq.SUB)
  defer context.Close()
  defer socket.Close()
  
  var temps []string
  var err os.Error
  var temp int
  total_temp := 0
  filter := "59937"
  
  // find zipcode
  if len(os.Args) > 1 {  // ./wuclient 85678
    filter = string(os.Args[1])
  }
  
  //  Subscribe to just one zipcode (whitefish MT 59937) 
  fmt.Printf("Collecting updates from weather server for %s…\n", filter)
  socket.SetSockOptString(zmq.SUBSCRIBE, filter)
  socket.Connect("tcp://localhost:5556")
  
  for i := 0; i < 101; i++ {
    // found temperature point
    datapt, _ := socket.Recv(0)
    temps = strings.Split(string(datapt)," ")
    temp, err = strconv.Atoi(temps[1])
    if err == nil { 
      // Invalid string 
      total_temp += temp
    }
  }
  
  fmt.Printf("Average temperature for zipcode %s was %dF \n\n", filter, total_temp / 100)
}