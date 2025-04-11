import std/async
import std/time/duration
import std/time/instant
import std/time/timestamp

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