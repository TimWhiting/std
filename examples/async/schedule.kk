import std/async
import std/time/duration
fun main()
  with async/handle
  try {
      interleaved { 
          trace("Interleavedx")
          debug-evv()
          wait(1.seconds)
          () 
        } { 
          ()
        }
      ()
    } fn(err) {
      println("error")
    }