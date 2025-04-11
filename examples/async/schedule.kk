import std/async
import std/time/duration
import std/time/instant
import std/time/timestamp

fun main()
  with async/handle
  try {
      interleaved { 
          trace("Main: wait")
          debug-evv()
          wait(1.seconds)
          trace("Here")
        } { 
          trace("There")
        }
      ()
    } fn(err) {
      trace("error")
    }