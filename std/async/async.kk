/*---------------------------------------------------------------------------
  Copyright 2012-2021, Microsoft Research, Daan Leijen.

  This is free software; you can redistribute it and/or modify it under the
  terms of the Apache License, Version 2.0. A copy of the License can be
  found in the LICENSE file at the root of this distribution.
---------------------------------------------------------------------------*/

/* Asynchronous primitives

This module is based closely on [@Leijen:async] and aims to have robust and composable asynchronous
primitives. In particular, any outstanding asynchronous operation can be canceled (through `cancel`)
within a certain scope which allows for composable primitives like `timeout` and `firstof`.


## References {-
~ Bibliography { caption:"0"~~ BibItem { #Leijen:async; bibitem-label:"[1]"; searchterm:"Leijen+Daan+Structured+Asynchrony+with+Algebraic+Effects"Daan Leijen.
_Structured Asynchrony with Algebraic Effects_.
Microsoft Research technical report MSR-TR-2017-21, May 2017.
[pdf](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/05/asynceffects-msr-tr-2017-21.pdf)
~~
~
\/
*/
module std/async/async

import std/data/dict
import std/data/array
import std/num/int32
import std/num/ddouble    // for C# backend
import std/time/duration
import std/time/timer
import std/async/null
pub import std/async/scheduler
import std/async/timer
import std/core/unsafe
pub import std/num/ddouble
pub import std/num/float64
import std/time/timestamp

  // js is just using primitives

// A type alias for asynchronous operations that can raise exceptions non-deterministically.
// This is common for almost all `:async` operations since `cancel` and `timeout` can
// cancel operations non-deterministically which raises the `Cancel` exception and cancels
// outstanding asynchronous requests.
pub alias asyncx = <async,exn,ndet>

pub alias async-exn = <async,exn>

alias setup-timeout = <io-noexn,schedule>

// ----------------------------------------------------------------------------
// Channels
// ----------------------------------------------------------------------------

// A _channel_ of values of type `:a`. Values can be asynchronously `emit`ed into
// a channel, and asynchronously `receive`d.
abstract value struct channel<a>(
  chid : int,
  state : ref<global,channel-state<a>>
)

// todo: use queue data type for the values and listeners for better complexity
abstract type channel-state<a>
  Empty
  Values( value : a, values : list<a> = [] )
  Waiting( listener : a -> io-noexn (), listeners : list<a -> io-noexn ()> = [] )

fun from-values(values : list<a> ) : channel-state<a>
  match values
    Nil -> Empty
    Cons(v,vs)  -> Values(v,vs)

fun from-waiting(listeners : list<a -> io-noexn ()>) : channel-state<a>
  match listeners
    Nil -> Empty
    Cons(l,ls)  -> Waiting(l,ls)

// Create a new asynchronous channel.
pub fun channel() : async channel<a>
  async-iox
    Channel(unique(), ref(Empty))

fun receivex( ch : channel<a>, cancelable : bool = True ) : <async,ndet> error<a>
  fun setup( cb : (_,_) -> io-noexn () )
    fun cbr(x) cb(Ok(x),True)    
    val r = ch.state
    match !r
      Empty -> r := Waiting(cbr,[])
      Waiting(l,ls) -> r := Waiting(l,ls ++ [cbr])
      Values(v,vs)->   // this case should not happen due to `try-receive`
        r := from-values(vs)
        cbr(v)
    Nothing

  match ch.try-receive
    Just(v) -> Ok(v)
    Nothing -> do-await(setup,empty-scope,cancelable)

// Return immediately if a value is available on the channel and otherwise returns `Nothing`.
pub fun try-receive( ch : channel<a> ) : <async,ndet> maybe<a>
  async-io-noexn
    val r = ch.state
    match (!r)
      Values(v, vs) ->
        r := from-values(vs)
        Just(v)
      _ -> Nothing

fun emit-io( ch : channel<a>, value : a ) : io-noexn ()
  val r = ch.state
  match !r
    Empty -> r := Values(value, [])
    Values(v,vs)  -> r := Values(v,vs ++ [value])
    Waiting(l,ls) ->
      r := from-waiting(ls)
      l(value)

// Emit a value asynchronously into a channel.
pub fun emit( ch : channel<a>, value : a ) : asyncx ()
  async-io
    emit-io(ch,value)

// ----------------------------------------------------------------------------
// Asynchronous timeout and waiting
// ----------------------------------------------------------------------------  

// Wait (asynchronously) for optional `secs` seconds `:duration` (`= 0.seconds`).
// Use `yield()` to yield generally to other asynchronous operations.
pub fun duration/wait( secs : duration = zero) : asyncx ()
  if secs <= zero then return yield()
  val msecs = max(zero:int32,secs.milli-seconds.int32)
  await fn(cb)
    val tid = async/set-timeout( fn(){ cb(Ok(())) }, msecs )
    Just( { async/clear-timeout(tid) } )

// Yield to other asynchronous operations. Same as `wait(0)`.
pub fun yield() : <asyncx> ()
  await0 fn(cb)
    schedule-timeout( cb, int32/zero )
    ()

// abstract wid for timeout handlers
abstract struct timeout-id(
  timer : any
)

alias unit-cb = () -> io-noexn ()

fun async/set-timeout( cb : unit-cb, ms : int32) : setup-timeout timeout-id
  debug-evv()
  Timeout-id(schedule-timeout(cb,max(ms,zero)))


fun async/clear-timeout( tid : timeout-id) : setup-timeout ()
  schedule-clear-timeout(tid.timer)

// ----------------------------------------------------------------------------
// Interleaved strands of execution
// ----------------------------------------------------------------------------

// Interleave two actions around their asynchronous operations.
pub fun two/interleaved( action1 : () -> <async-exn|e> a, action2 : () -> <async-exn|e> b ) : <async-exn|e> (a,b)
  val (ra,rb) = interleavedx( {mask behind<exn>(action1)}, {mask behind<exn>(action2)} )
  [ra.maybe-exn,rb.maybe-exn].ordered_throw
  (ra.untry,rb.untry)

fun maybe-exn( err : error<a> ) : maybe<exception>
  match err
    Error(exn) -> Just(exn)
    _          -> Nothing

fun ordered_throw( xs : list<maybe<exception>> ) : exn ()
  var mexn := Nothing
  xs.foreach fn(x)
    match x
      Nothing -> ()
      Just(exn) -> match mexn
        Nothing -> mexn := x
        Just(exnx) ->
          if ((exn.is-finalize && !exnx.is-finalize) || (exnx.is-cancel && !exn.is-cancel))
            then mexn := x
  match mexn
    Just(exn)  -> rethrow(exn)
    Nothing  -> ()

// Interleave two actions around their asynchronous operations and explicitly returning either
// their result or their exception.
pub fun interleavedx( action1 : () -> <async-exn|e> a, action2 : () -> <async-exn|e> b ) : <async|e> (error<a>,error<b>)
  fun act1() Left(action1())  
  fun act2() Right(action2())  
  match interleavedx([act1,act2])  
    Cons(x,Cons(y)) -> (x.unleft,y.unright)
    _ ->
      // this will never happen..
      val exn = Exception("invalid interleaved result",ExnInternal("std/async/interleavedx(action1,action2)"))
      (Error(exn),Error(exn))

fun unleft( x : error<either<a,b>> ) : error<a>
  match x
    Ok(Left(l)) -> Ok(l)
    Error(exn)  -> Error(exn)
    _ -> Error(Exception("invalid left interleaved result",ExnInternal("std/async/interleavedx(action1,action2)")))
fun unright( x : error<either<a,b>> ) : error<b>
  match x
    Ok(Right(r)) -> Ok(r)
    Error(exn)     -> Error(exn)
    _ -> Error(Exception("invalid right interleaved result",ExnInternal("std/async/interleavedx(action1,action2)")))

// Private effect to keep track of when a strand in an interleaving is done.
// Preferred over using built-in state as this works well if there is an outer handler
// over the state that resumes more than once -- redoing part of the interleaving.
// See `test/algeff/async5.js`
effect strands<a>
  // Are there still strands that need to be resumed?
  fun strands-are-busy() : bool
  // Call this when a strand is done.
  fun strand-done(idx : int, result : error<a>) : ()

// Insert in order with an accumulating list.
fun insert-acc( xs : list<(int,a)>, idx : int, value : a, acc : list<(int,a)> ) : list<(int,a)>
  match xs
    Cons(x,xx) | x.fst < idx -> insert-acc(xx, idx, value, Cons(x,acc))
    _ -> reverse-append( acc, Cons((idx,value),xs) )

// Insert in order
fun insert( xs : list<(int,a)>, idx : int, value : a, n : int  = 0 ) : list<(int,a)>
  if n > 100
    then insert-acc( xs, idx, value, [] )
    else match xs
      Cons(x,xx) | x.fst < idx -> Cons(x, insert(xx, idx, value, n + 1))
      _ -> Cons((idx,value),xs)

pub inline extern debug-evv(?kk-file-line: string): e ()
  js inline "console.log(#1, $std_core_hnd._evv_get());"

// Interleave a list actions around their asynchronous operations and explicitly returning either
// either their result or their exception.
pub fun list/interleavedx( xs : list<() -> <async-exn|e> a> ) : <async|e> list<error<a>>
  val n = xs.length
  if n==0 then []
  elif n==1 then xs.map(unsafe-try-all)
  else interleavedn(n,xs)

fun interleavedn( n : int, xs : list<() -> <async-exn|e> a> ) : <async|e> list<error<a>> 
  unsafe-no-ndet-div  
    var cr : some<a> (int,list<(int,error<a>)>) := (n,[])
    with handler 
      return(x) 
        cr.snd.map( snd )
      fun strands-are-busy()   
        cr.fst > 0    
      fun strand-done(idx,res) 
        cr := (cr.fst - 1, cr.snd.insert(idx,res)) 
    mask<local>{interleaved-div(xs)} 


inline extern unsafe-no-ndet-div-cast : forall<a,e> (() -> <ndet,div|e> a) -> (() -> e a) 
  inline "#1" 

fun unsafe-no-ndet-div( action : () -> <ndet,div|e> a ) : e a 
  unsafe-no-ndet-div-cast(action)()

inline extern inject-ndet-div : forall<a,h,e> (() -> e a) -> total (() -> <ndet,div|e> a)   
  inline "#1"

fun error/is-finalize( t : error<a> ) : bool
  match t
    Error(exn) -> exn.is-finalize
    _ -> False

pub fun error/is-cancel( t : error<a> ) : bool
  match t
    Error(exn) -> exn.is-cancel
    _ -> False

fun interleaved-div( xs : list<() -> <async-exn|e> a> ) : <async,ndet,div,strands<a>|e> ()
  val strands =  xs.map-indexed fn(i,action)
      return fn()
        val res = unsafe-try-all(inject-ndet-div({mask<strands>{action()}}))
        strand-done(i,res)
        if res.is-finalize then cancel()  // cancel others if finalization happens
  val ch : some<a,e> channel<() -> <async|e> a> = channel()
  fun handle-strand(action) 
    with override     
      // redirect all other operations
      fun no-await(setup,scope,c,f)  
        no-await(setup,scope,c,f)    
      fun async-iox(f)  
        async-iox(f)    
      fun cancel(scope)  
        cancel(scope)
      raw ctl do-await(setup,scope,c)
        no-await( setup, scope, c) fn(res)
          // emit a resumption of this strand into the channel
          ch.emit-io( /* no-cps */ { rcontext.resume(res) }  )
        ()  // stop the strand at this point 
    action()
  debug-evv()
  strands.foreach fn(strand)
    handle-strand{ 
      strand()
    }

  while { strands-are-busy() } // while there are resumptions on the strands..
    // the only way receive can throw is through a cancelation -- but in that case
    // we should not cancel but instead await all canceled strands; so keep listening on the channel.
    match(ch.receivex(False))
      Error(_exn) -> () // ignore cancelation on receive
      Ok(strand-resume) -> strand-resume()
  ()



// ----------------------------------------------------------------------------
// Await wrappers
// ----------------------------------------------------------------------------

// Convenience function for awaiting a zero argument callback.
pub fun await0( setup : (cb : () -> io-noexn () ) -> <schedule,io> () ) : <asyncx> ()
  await fn(cb) 
    setup( fn() cb(Ok(())) )
    Nothing 

// Execute `setup` to set up an asynchronous callback with the host platform. Invoke `cb` as the callback:
// it takes either an exception or a result `a`. Usually `setup` returns `Nothing` but you can return a `Just(cleanup)`
// value where the `cleanup` functions is invoked on cancellation to dispose of any resources (see the implementation of `wait`).
// The callback should be invoked exactly once -- when that happens `await` is resumed with the result using `untry`
// either raise an exception or return the plain result.
pub fun setup/await( setup : (cb : error<a> -> io-noexn () ) -> <schedule,io> maybe<() -> <schedule,io-noexn> ()> ) : <asyncx> a
  await-exn(setup).untry

// ----------------------------------------------------------------------------
// Async effect
// ----------------------------------------------------------------------------
alias await-result<a> = error<a>
alias await-setup<a> = (cb : (error<a>,bool) -> <io-noexn> ()) -> <schedule,io-noexn> (maybe<() -> <schedule,io-noexn> ()>)


// Asynchronous operations have the `:async` effect.
pub effect async
  ctl do-await( setup : await-setup<a>, scope : scope, cancelable : bool ) : error<a>
  ctl no-await( setup : await-setup<a>, scope : scope, cancelable : bool, f : error<a> -> io-noexn ()  ) : ()
  ctl async-iox( action : () -> io-noexn a ) : a
  ctl cancel( scope : scope ) : ()

// The `cancel` operations cancels any outstanding asynchronous operation under the innermost
// `cancelable` handler by returning the `Cancel` exception. The `cancel` operation itself returns normally
// without raising a `Cancel` exception.
pub fun noscope/cancel() : async ()
  cancel(empty-scope)

// Primitive: Execute `setup` to set up an asynchronous callback with the host platform. Invoke `cb` as the callback:
// it takes either an exception or a result `a`. Usually `setup` returns `Nothing` but you can return a `Just(cleanup)`
// value where the `cleanup` functions is invoked on cancellation to dispose of any resources (see the implementation of `wait`).
// The callback should be invoked exactly once -- when that happens `await-exn` is resumed with the result.
pub fun await-exn( setup : (cb : (error<a>) -> io-noexn ()) -> <schedule,io> (maybe<() -> <schedule,io-noexn> ()>) ) : async error<a>
  do-await(fn(cb)
    match (try{ setup(fn(res) cb(res,True) ) })
      Ok(mcleanup) -> mcleanup
      Error(exn) ->
        cb(Error(exn),True)
        Nothing
  , empty-scope, True)

fun async-io-noexn( f : () -> io-noexn a ) : <async,ndet> a
  async-iox(f)

// Perform an I/O operation at the outer level; exceptions are propagated back.
fun async-io( f : () -> io a ) : asyncx a
  async-io-noexn( { try(f) } ).untry

// ----------------------------------------------------------------------------
// Async handle
// ----------------------------------------------------------------------------

pub fun @default-async(action)
  async/handle(action)

fun nodispose() : io-noexn () 
   ()

pub fun async/synthetic-time(action : () -> <async,io-noexn|e> b): <async,io-noexn|e> b
  val test-scheduler = make-test-scheduler()
  handle <async>({ mask behind<async>(action) })
    fun do-await(setup, scope, c)
      fun setup-handled(cb)
        // wrap the setup and teardown functions in an inner `test-scheduler`,
        // which will override the default behaviour from the parent <async>
        val cleanup = test-scheduler.handle-schedule { setup(cb) }
        match cleanup
          Nothing -> Nothing
          Just(cleanup) -> Just(fn() test-scheduler.handle-schedule(cleanup))
      do-await(setup-handled, scope, c)

    fun no-await( setup, scope, c, f ) no-await(setup, scope, c, f)
    fun cancel( scope ) cancel(scope)
    fun async-iox( f ) async-iox(f)

// The outer `:async` effect handler. This is automatically applied by the compiler
// around the `main` function if it has an `:async` effect.
pub fun async/handle(action : () -> <async,io-noexn> () ) : io-noexn ()
  val callbacks : ref<global,list<(scope,() -> io-noexn ())>> = unsafe-total{ref([])}
  fun handle-schedule(schedule-action: () -> <schedule,io-noexn|e> a): <io-noexn|e> a
    handle <schedule>(schedule-action)
      fun schedule-timeout(tf, t) -> raw/set-timeout(tf, t)
      fun schedule-clear-timeout(t) -> raw/clear-timeout(t)

  fun handle-await( setup : await-setup<a>, scope : scope, f : error<a> -> io-noexn (), cancelable : bool) : <io-noexn> ()
    val cscope = child-scope(unique(),scope)
    val dispose = ref(nodispose)
    fun cb( res : error<_>, is-done : bool ) : io-noexn ()
      if ((!callbacks).contains(cscope)) then
        if is-done then
          callbacks := (!callbacks).remove(cscope)
          if res.is-error then try(!dispose).default(())
        f(res)

    // trace("register: " + cscope.show)
    callbacks := Cons((cscope, if cancelable then fn(){ cb(Error(Exception("cancel",Cancel)),True) } else nodispose), !callbacks)
    try {
      // setup the callback which returns a possible dispose function
      // We invoke the callback with real-timeouts, however
      // `custom-schedule` may override the inner `schedule` handle to inject
      // a different implementation
      with handle-schedule
      match(setup(cb))
        Just(d) -> dispose := (fn() handle-schedule { d() })
        Nothing -> ()
    } fn(exn)
      // if setup fails, immediately resume with the exception
      cb(Error(exn),True)

  fun handle-cancel( scope : scope ) : io-noexn ()
    (!callbacks).foreach fn(entry)
      val (cscope,cb) = entry
      if (cscope.in-scope-of(scope)) then cb()
  handle <async> (action)
    raw ctl do-await( setup, scope, c ) 
      handle-await(setup,scope, 
                   fn(x) {
                     rcontext.resume(x)}, c)   // returns to outer event loop
    fun no-await( setup, scope, c, f ) 
      handle-await(setup,scope,f,c)
    fun cancel( scope ) 
      handle-cancel(scope)
    fun async-iox( f ) 
      f()

fun io-noexn( f : () -> io-noexn a ) : io a
  f()

fun io-noexn1( f : (a1) -> io-noexn a, x1 : a1 ) : io a
  f(x1)

// ----------------------------------------------------------------------------
// Scope identifiers
// ----------------------------------------------------------------------------

abstract struct scope( : list<int> )

val empty-scope = Scope([])

fun child-scope( id : int, scope : scope ) : scope
  match scope
    Scope(cids) -> Scope(cids ++ [id])

fun ids/in-scope-of( child : list<int>, parent : list<int> ) : bool
  match parent
    Nil -> True
    Cons(p,ps) -> match child
      Cons(c,cs) -> (c == p && in-scope-of(cs,ps))
      Nil -> False

fun in-scope-of( child : scope, parent : scope ) : bool
  match parent
    Scope(pids) -> match child
      Scope(cids) -> in-scope-of(cids,pids)

fun scope/(==)(scope1 : scope, scope2 : scope ) : bool
  match scope1
    Scope(ids1) -> match scope2
      Scope(ids2) -> ids1==ids2

// Convenience functions for scope maps
fun remove( xs : list<(scope,a)>, scope : scope ) : list<(scope,a)>
  xs.remove( fn(x:(scope,_)) { x.fst == scope })

fun lookup( xs : list<(scope,a)>, scope : scope ) : maybe<a>
  xs.lookup( fn(x:scope) { x == scope })

fun contains( xs : list<(scope,a)>, scope : scope ) : bool
  xs.lookup(scope).bool

abstract extend type exception-info
  con Cancel
  con Finalize(yld:yield-info)

// Was this a cancelation exception?
fun exn/is-cancel( exn : exception ) : bool
  match exn.info
    Cancel -> True
    _      -> False

// Was this a finalization exception?
fun exn/is-finalize(exn : exception) : bool
  match exn.info
    Finalize -> True
    _ -> False

fun unsafe-try-all( action : () -> <exn|e> a ) : e error<a>
  val fin = unsafe-try-finalize{ try(action) } 
  match fin
    Right(t)  -> t
    Left(yld)  -> Error(Exception("finalize",Finalize(yld)))

fun rethrow( exn : exception ) : exn a
  match exn.info
    Finalize(yld) -> unsafe-reyield(yld)
    _             -> throw-exn(exn)
