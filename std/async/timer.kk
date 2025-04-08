/*----------------------------------------------------------------------------
   Copyright 2024, Koka-Community Authors

   Licensed under the MIT License ("The License"). You may not
   use this file except in compliance with the License. A copy of the License
   can be found in the LICENSE file at the root of this distribution.
----------------------------------------------------------------------------*/

module std/async/timer
import std/num/int32

abstract struct timer (
  internal: any
)

// Sets a timeout for libuv / wasm / javscript / C# event loops
pub extern raw/set-timeout( cb : () -> io-noexn (), ms : int32 ) : io-noexn any
  js "setTimeout"

// Clears a timeout for libuv / wasm / javscript / C# event loops
pub extern raw/clear-timeout( tid : any) : io-noexn ()
  js "clearTimeout"
