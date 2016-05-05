namespace Nessos.Streams
open System
open System.Collections.Generic
open System.Threading

/// Provides on-demand iteration 
type internal Iterator = 
    /// Function for on-demand processing
    abstract TryAdvance : unit -> bool 
    /// Cleanup function
    abstract Dispose : unit -> unit 

/// Provides functions for iteration
type internal Iterable = 
    /// Function for bulk processing
    abstract Bulk : unit -> unit 
    /// Iterator for on-demand processing
    abstract Iterator : Iterator

[<AllowNullLiteral>]
/// An internal type used for the inlined implementation of this API
type internal StreamCancellationTokenSource() = 
     let mutable cancelled = false
     let mutable linked : StreamCancellationTokenSource list = []
     member x.CreateLinkedTokenSource() = let sc = StreamCancellationTokenSource() in linked <- sc :: linked; sc
     member x.Cancel() = cancelled <- true; for l in linked do l.Cancel()
     member x.Cancelled = cancelled

/// Represents the current executing contex
type internal Context<'T> = {
    /// The composed continutation
    Cont : 'T -> unit
    /// The completed continuation
    Complete : unit -> unit
    /// The current CancellationTokenSource
    Cts : StreamCancellationTokenSource 
}

/// Represents a Stream of values.
type Stream<'T> = 
    internal { Run : Context<'T> -> Iterable } 
    member inline internal stream.RunBulk ctxt = (stream.Run ctxt).Bulk()
    override self.ToString() = 
        seq {
            use enumerator = new StreamEnumerator<'T>(self) :> IEnumerator<'T>
            while enumerator.MoveNext() do
                yield enumerator.Current
        } |> sprintf "%A"

// Wraps stream as a IEnumerable
and private StreamEnumerator<'T> (stream : Stream<'T>) =
    let results = new ResizeArray<'T>()
    let mutable index = -1
    let mutable count = 0
    let iterable = 
        stream.Run 
          { Complete = (fun () -> ()); 
            Cont =  (fun v -> 
                        let currentIndex = count
                        count <- count + 1
                        if count <= results.Count then
                            results.[currentIndex] <- v
                        else
                            results.Add(v); 
                        ()); 
            Cts = null }
    let iterator = iterable.Iterator

    interface System.Collections.IEnumerator with
        member __.Current = box results.[index]
        member __.MoveNext () =
            let rec awaitNext () =
                index <- index + 1
                if index >= count then
                    count <- 0
                    if iterator.TryAdvance() then
                        if count > 0 then 
                            index <- 0
                            true
                        else 
                            awaitNext ()
                    else
                        false
                else
                    true

            awaitNext ()

        member __.Reset () = raise <| new NotSupportedException()

    interface IEnumerator<'T> with
        member __.Current = results.[index]
        member __.Dispose () = iterator.Dispose()

/// Provides basic operations on Streams.
[<RequireQualifiedAccessAttribute>]
module Stream =

    let inline  internal Stream f = { Run = f }

    module Internals = 
        // Public permanent entrypoint to implement inlined versions of map, filter, choose etc.
        // 'f' is called with one argument before iteration.
        let mapCont f stream =
            Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
                stream.Run { Complete = complete; 
                             Cont = f iterf; 
                             Cts = cts })

        // Public permanent entrypoint to implement inlined versions of takeWhile etc.
        // 'f' is called with two arguments before iteration.
        let mapContCancel f stream =
            Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
                let cts = if cts = null then StreamCancellationTokenSource() else cts 
                stream.Run 
                        { Complete = complete;
                          Cont = f cts.Cancel iterf;
                          Cts = cts })

        // Public permanent entrypoint to implement inlined versions of linked iteration in ParStreams
        // Iterates using a new cancellation source linked to the given source.
        let iterCancelLink (cts: CancellationTokenSource) (f : ('T -> unit)) (stream : Stream<'T>) : unit = 
           let cts' = StreamCancellationTokenSource()
           cts.Token.Register(fun _ -> cts'.Cancel()) |> ignore
           stream.RunBulk
                { Complete = (fun () -> ())
                  Cont = f 
                  Cts = cts' } 

        // Public permanent entrypoint to implement inlined versions of tryFind, tryPick
        // Iterates using a new cancellation source and passes the cancel function to the caller when computing Cont.
        let iterCancel (f : (unit -> unit) -> ('T -> unit)) (stream : Stream<'T>) : unit = 
           let cts = StreamCancellationTokenSource()
           stream.RunBulk
                { Complete = (fun () -> ())
                  Cont = f cts.Cancel
                  Cts = cts } 

    /// <summary>The empty stream.</summary>
    /// <returns>An empty stream.</returns>
    let empty<'T> : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            { new Iterable with 
                 member __.Bulk() = ()
                 member __.Iterator = 
                    { new Iterator with 
                         member __.TryAdvance() = false
                         member __.Dispose () = ()} })

    /// <summary>Creates a singleton stream.</summary>
    /// <param name="source">The singleton stream element</param>
    /// <returns>A stream of just the given element</returns>
    let singleton (source: 'T) : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts }->
            let pulled = ref false
            { new Iterable with 
                 member __.Bulk() = iterf source; complete ()
                 member __.Iterator = 
                     { new Iterator with 
                          member __.TryAdvance() = 
                            if !pulled then false
                            else
                                iterf source 
                                pulled := true
                                complete ()
                                true
                          member __.Dispose() = ()} })
           
    /// <summary>Wraps array as a stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result stream.</returns>
    let ofArray (source : 'T []) : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                if not <| obj.ReferenceEquals(cts, null) then
                    let mutable i = 0
                    while i < source.Length && not cts.Cancelled do
                        iterf source.[i]
                        i <- i + 1
                else
                    for i = 0 to source.Length - 1 do
                        iterf source.[i]
                complete ()
                
            let iterator() = 
                let i = ref 0
                let cts = if cts = null then StreamCancellationTokenSource() else cts 
                { new Iterator with 
                    member __.TryAdvance() = 
                        if cts.Cancelled then
                            false
                        else if !i < source.Length then
                            iterf source.[!i] 
                            if cts.Cancelled then
                                complete ()
                            incr i
                            true
                        else
                            cts.Cancel()
                            complete ()
                            true
                    member __.Dispose() = 
                        ()}
            { new Iterable with 
                 member __.Bulk() = bulk()
                 member __.Iterator = iterator() })

    /// <summary>Wraps list as a stream.</summary>
    /// <param name="source">The input list.</param>
    /// <returns>The result stream.</returns>
    let ofList (source : 'T list) : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                if not <| obj.ReferenceEquals(cts, null) then
                    let rec aux rest =
                        if cts.Cancelled then () else
                        match rest with
                        | [] -> ()
                        | t :: tl -> iterf t ; aux tl

                    aux source
                else
                    let rec aux rest =
                        match rest with
                        | [] -> ()
                        | t :: tl -> iterf t ; aux tl

                    aux source

                complete ()
                
            let iterator() = 
                let tail = ref source
                let cts = if cts = null then StreamCancellationTokenSource() else cts 
                { new Iterator with 
                    member __.TryAdvance() = 
                        if cts.Cancelled then
                            false
                        else 
                            match !tail with
                            | [] -> cts.Cancel() ; complete () ; true
                            | hd :: tl ->
                                iterf hd
                                if cts.Cancelled then complete ()
                                tail := tl
                                true

                    member __.Dispose() = 
                        ()}
            { new Iterable with 
                 member __.Bulk() = bulk()
                 member __.Iterator = iterator() })

    /// <summary>Wraps ResizeArray as a stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result stream.</returns>
    let ofResizeArray (source : ResizeArray<'T>) : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                if not (cts = null) then
                    let mutable i = 0
                    while i < source.Count && not cts.Cancelled do
                        iterf source.[i]
                        i <- i + 1
                else
                    for i = 0 to source.Count - 1 do
                        iterf source.[i]
                complete ()

            let iterator() = 
                let i = ref 0
                let continueFlag = ref true
                let cts = if cts = null then StreamCancellationTokenSource() else cts 
                { new Iterator with 
                    member __.TryAdvance() = 
                        if cts.Cancelled then
                            false
                        else if !i < source.Count then
                            iterf source.[!i] 
                            if cts.Cancelled then
                                complete ()
                            incr i
                            true
                        else
                            cts.Cancel()
                            complete ()
                            true

                    member __.Dispose() = 
                        ()}

            { new Iterable with 
                 member __.Bulk() = bulk()
                 member __.Iterator = iterator() })

    /// <summary>Wraps seq as a stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result stream.</returns>
    let ofSeq (source : seq<'T>) : Stream<'T> =
        match source with
        | :? ('T[]) as array -> ofArray array
        | :? ('T list) as list -> ofList list
        | :? ResizeArray<'T> as list -> ofResizeArray list
        | _ -> 
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                if not (cts = null) then
                    use enumerator = source.GetEnumerator()
                    while enumerator.MoveNext() && not cts.Cancelled do
                        iterf enumerator.Current
                    complete ()
                else
                    for value in source do
                        iterf value
                    complete ()

            let iterator() = 
                let enumerator = source.GetEnumerator()
                let cts = if cts = null then StreamCancellationTokenSource() else cts 
                { new Iterator with 
                     member __.TryAdvance() = 
                        if cts.Cancelled then
                            false
                        else if enumerator.MoveNext() then
                            iterf enumerator.Current
                            if cts.Cancelled then
                                complete ()
                            true
                        else
                            cts.Cancel()
                            complete ()
                            true
                     member __.Dispose() = 
                         ()
                         enumerator.Dispose() }
            { new Iterable with 
                 member __.Bulk() = bulk() 
                 member __.Iterator = iterator()  })

    /// <summary>
    ///     Produces a stream by sequentially reading a System.IO.Stream
    ///     of text, splitting input by line separator.
    /// </summary>
    /// <param name="stream">Input System.IO.Stream.</param>
    let ofSystemStreamByLine (stream : System.IO.Stream) : Stream<string> =
        let lines = TextReaders.ReadLines(stream, disposeStream = true)
        ofSeq lines

    /// <summary>
    ///     Produces a stream by sequentially reading a text file from given path.
    ///     Text file entries are split by line separator.
    /// </summary>
    /// <param name="path">Path to local text file.</param>
    let ofTextFileByLine (path : string) : Stream<string> =
        ofSystemStreamByLine (System.IO.File.OpenRead(path))

    /// <summary>Produces an infinite Stream by calling the given function.</summary>
    /// <param name="generator">A function used to generate values.</param>
    /// <returns>The result stream.</returns>
    let generateInfinite (generator : unit -> 'T) : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                if not (cts = null) then
                    while not cts.Cancelled do
                        iterf (generator())
                else
                    while true do
                        iterf (generator())
                complete ()
            let iterator() = 
                let cts = if cts = null then StreamCancellationTokenSource() else cts 
                { new Iterator with 
                     member __.TryAdvance() = 
                        if cts.Cancelled then
                            false
                        else 
                            iterf (generator())
                            if cts.Cancelled then
                                complete ()
                            true
                     member __.Dispose() = 
                         () }
            { new Iterable with 
                 member __.Bulk() = bulk() 
                 member __.Iterator = iterator()  })


    /// <summary>
    ///     Generates an infinite Stream which returns successive elements by calling the given function.
    ///     Indexing starts at 0, and continues until Int32.MaxValue
    /// </summary>
    /// <param name="initializer">A function that generates an item in the sequence from a given index.</param>
    /// <returns>The result stream.</returns>
    let initInfinite (initializer : int -> 'T) : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                let mutable index = 0
                if not (cts = null) then
                    while not cts.Cancelled do
                        iterf (initializer index)
                        index <- index + 1
                else
                    while true do
                        iterf (initializer index)
                        index <- index + 1
                complete ()
            let iterator() = 
                let cts = if cts = null then StreamCancellationTokenSource() else cts 
                let index = ref 0
                { new Iterator with 
                     member __.TryAdvance() = 
                        if cts.Cancelled then
                            false
                        else 
                            iterf (initializer(index.Value))
                            index.Value <- index.Value + 1
                            if cts.Cancelled then
                                complete ()
                            true
                     member __.Dispose() = 
                         () }
            { new Iterable with 
                 member __.Bulk() = bulk() 
                 member __.Iterator = iterator()  })
        
    /// <summary>Wraps an IEnumerable as a stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result stream.</returns>
    let cast<'T> (source : System.Collections.IEnumerable) : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                
                if not (cts = null) then
                  let enumerator = source.GetEnumerator() // not disposable
                  try
                    while enumerator.MoveNext() && not cts.Cancelled do
                        iterf (enumerator.Current :?> 'T)
                    complete ()
                  finally
                    match enumerator with 
                    | :? System.IDisposable as disposable -> disposable.Dispose()
                    | _ -> ()                    
                else
                    for value in source do
                        iterf (value :?> 'T)
                    complete ()

            let iterator() = 
                let enumerator = source.GetEnumerator()
                let cts = if cts = null then StreamCancellationTokenSource() else cts 
                { new Iterator with 
                     member __.TryAdvance() = 
                        if not cts.Cancelled && enumerator.MoveNext()  then
                            iterf (enumerator.Current :?> 'T)
                            true
                        else
                            false
                     member __.Dispose() = 
                        match enumerator with 
                        | :? System.IDisposable as disposable -> disposable.Dispose()
                        | _ -> ()
                        complete ()
                        () }
            { new Iterable with 
                member __.Bulk() = bulk()
                member __.Iterator = iterator()  })

    // Used to indicate that we don't want a closure to be curried
    let inline internal nocurry() = Unchecked.defaultof<unit>

    /// <summary>Transforms each element of the input stream.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        stream |> Internals.mapCont (fun iterf -> nocurry(); fun value -> iterf (f value))


    /// <summary>Transforms each element of the input stream. The integer index passed to the function indicates the index (from 0) of element being transformed.</summary>
    /// <param name="f">A function to transform items and also supplies the current index.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline mapi (f : int -> 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        stream |> Internals.mapCont (fun iterf -> let counter = ref -1 in (fun value -> incr counter; iterf (f !counter value))) 

    /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let flatMap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            stream.Run
                    { Complete = complete;
                      Cont =  
                        (fun value -> 
                            let stream' = f value;
                            let cts' = if cts = null then cts else cts.CreateLinkedTokenSource()
                            stream'.RunBulk { Complete = (fun () -> ()); Cont = iterf; Cts = cts' } );
                      Cts = cts })

    /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatMap f stream

    /// <summary>Creates a cached version of the input stream.</summary>
    /// <param name="source">The input stream.</param>
    /// <returns>The cached stream.</returns>
    let cache (source: Stream<'T>): Stream<'T> =
        let cache = new ResizeArray<'T>()
        //if !cached = None && cache.Count = 0 then the stream is not cached
        //if !cached = None && cache.Count > 0 then the stream is partially cached
        //if !cached = Some then the stream is fully cached
        let cached = ref None : Stream<'T> option ref
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            if Option.isSome !cached then
                //fully cached case
                let stream = (!cached).Value 
                stream.Run { Complete = complete; Cont = iterf; Cts = cts }
            else //partially cached or not cached at all case
                let count = ref 0
                let iterable = 
                     source.Run { Complete = complete;
                                  Cont = (fun v -> (if cache.Count - !count = 0 then cache.Add(v)); incr count; iterf v);
                                  Cts = cts }
                let bulk' () = lock cache (fun () -> iterable.Bulk(); cached := Some (ofResizeArray cache))
                let iterator'() = 
                    let iterator = iterable.Iterator 
                    { new Iterator with 
                        member __.TryAdvance() = 
                            //locking each next() seem's overkill
                            lock cache (fun () -> 
                                if iterator.TryAdvance() then true 
                                else cached := Some (ofResizeArray cache); false)
                        member __.Dispose() = iterator.Dispose() } 
                { new Iterable with 
                    member __.Bulk() = bulk'(); 
                    member __.Iterator = iterator'() })


    /// <summary>Filters the elements of the input stream.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline filter (predicate : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        stream |> Internals.mapCont (fun iterf -> nocurry(); fun value -> if predicate value then iterf value)

    /// <summary>Applies the given function to each element of the stream and returns the stream comprised of the results for each element where the function returns Some with some value.</summary>
    /// <param name="chooser">A function to transform items of type 'T into options of type 'R.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline choose (chooser : 'T -> 'R option) (stream : Stream<'T>) : Stream<'R> =
        stream |> Internals.mapCont (fun iterf -> nocurry(); fun value -> match chooser value with | Some value' -> iterf value' | None -> ())

    /// <summary>Returns the elements of the stream up to a specified count.</summary>
    /// <param name="n">The number of items to take.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let take (n : int) (stream : Stream<'T>) : Stream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let counter = ref 0
            let cts = if cts = null then StreamCancellationTokenSource() else cts
            stream.Run 
                    { Complete = complete;
                      Cont = 
                        (fun value -> 
                            incr counter
                            if !counter < n then 
                                iterf value
                            else if !counter = n then
                                iterf value
                                cts.Cancel()
                            else cts.Cancel());
                      Cts = cts } )

    /// <summary>Returns the elements of the stream while the given predicate returns true.</summary>
    /// <param name="pred">The predicate function.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline takeWhile pred (stream : Stream<'T>) : Stream<'T> = 
        stream |> Internals.mapContCancel (fun cancel iterf -> nocurry(); fun value -> if pred value then iterf value else cancel())
        

    /// <summary>Returns a stream that skips N elements of the input stream and then yields the remaining elements of the stream.</summary>
    /// <param name="n">The number of items to skip.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let skip (n : int) (stream : Stream<'T>) : Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let counter = ref 0
            stream.Run 
                    { Complete = complete;
                      Cont = 
                        (fun value -> 
                            incr counter
                            if !counter > n then iterf value);
                      Cts = cts })


    /// <summary>Concatenates a collection of streams.</summary>
    /// <param name="streams">The sequence of streams to concatenate.</param>
    /// <returns>The concatenated stream.</returns>
    let concat (streams: #seq<Stream<'T>>): Stream<'T> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                for stream in streams do
                    stream.RunBulk { Complete = (fun () -> ()); Cont = iterf; Cts = cts }
                complete ()
            let iterator() =
                if Seq.isEmpty streams then 
                    { new Iterator with 
                        member __.TryAdvance() = false; 
                        member __.Dispose() = () }
                else                    
                    let cts = if cts = null then StreamCancellationTokenSource() else cts 
                    let enumerator =
                        let streams = 
                            streams
                            |> Seq.collect (fun stream ->         
                                             { new IEnumerable<'T> with
                                                member __.GetEnumerator () = new StreamEnumerator<'T>(stream) :> IEnumerator<'T>
                                                member __.GetEnumerator () = new StreamEnumerator<'T>(stream) :> System.Collections.IEnumerator })
                        streams.GetEnumerator()
                    { new Iterator with 
                        member __.TryAdvance() = 
                            if cts.Cancelled then
                                false
                            else if enumerator.MoveNext() then
                                iterf enumerator.Current 
                                if cts.Cancelled then
                                    complete ()
                                true
                            else 
                                cts.Cancel()
                                complete ()
                                true
                        member __.Dispose() = 
                            enumerator.Dispose() }

            { new Iterable with 
                member __.Bulk() = bulk()
                member __.Iterator = iterator() })



    /// <summary>Applies a specified function to the corresponding elements of two streams, producing a stream of the results.</summary>
    /// <param name="f">The combiner function.</param>
    /// <param name="first">The first input stream.</param>
    /// <param name="second">The second input stream.</param>
    /// <returns>The result stream.</returns>
    let zipWith (f : 'T -> 'S -> 'R) (first : Stream<'T>) (second : Stream<'S>) : Stream<'R> =
        Stream (fun { Complete = complete; Cont = iterf; Cts = cts } ->
            let bulk () =
                let firstEnumerator = new StreamEnumerator<'T>(first) :> IEnumerator<'T>
                let secondEnumerator = new StreamEnumerator<'S>(second) :> IEnumerator<'S>
                let cts = if cts = null then StreamCancellationTokenSource() else cts
                while not cts.Cancelled do
                    if firstEnumerator.MoveNext() && secondEnumerator.MoveNext() then
                        iterf (f firstEnumerator.Current secondEnumerator.Current)
                    else
                        cts.Cancel()
                    ()
                complete ()
            let iterator() = 
                let firstEnumerator = new StreamEnumerator<'T>(first) :> IEnumerator<'T>
                let secondEnumerator = new StreamEnumerator<'S>(second) :> IEnumerator<'S>
                let cts = if cts = null then StreamCancellationTokenSource() else cts
                { new Iterator with 
                    member __.TryAdvance() = 
                        if cts.Cancelled then
                            false
                        else if firstEnumerator.MoveNext() && secondEnumerator.MoveNext() then
                            iterf (f firstEnumerator.Current secondEnumerator.Current)
                            if cts.Cancelled then
                                complete ()
                            true
                        else
                            cts.Cancel()
                            complete ()
                            true
                    member __.Dispose() = 
                        firstEnumerator.Dispose()
                        secondEnumerator.Dispose() }
            { new Iterable with 
                member __.Bulk() = bulk()
                member __.Iterator = iterator() })


    /// <summary>Applies the given function to each element of the stream.</summary>
    /// <param name="f">A function to apply to each element of the stream.</param>
    /// <param name="stream">The input stream.</param>    
    let iter (f : 'T -> unit) (stream : Stream<'T>) : unit = 
       stream.RunBulk
            { Complete = (fun () -> ())
              Cont = f
              Cts = null } 

    /// <summary>Applies a function to each element of the stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the stream.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : 'State =
        let accRef = ref state
        stream |> iter (fun value -> accRef := folder !accRef value)
        !accRef

    /// <summary>Like Stream.fold, but computes on-demand and returns the stream of intermediate and final results</summary>
    /// <param name="folder">A function that updates the state with each element from the stream.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The final stream.</returns>
    let inline scan (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : Stream<'State> =
        stream |> Internals.mapCont (fun iterf -> 
            let accRef = ref state
            iterf !accRef
            (fun value -> 
                accRef := folder !accRef value
                iterf !accRef ))

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (stream : Stream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) LanguagePrimitives.GenericZero stream

    /// <summary>Returns the total number of elements of the stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The total number of elements.</returns>
    let length (stream : Stream<'T>) : int =
        fold (fun acc _  -> 1 + acc) 0 stream

    /// <summary>Creates an Seq from the given stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result Seq.</returns>    
    let toSeq (stream : Stream<'T>) : seq<'T> =
        {
            new IEnumerable<'T> with
                member __.GetEnumerator () = new StreamEnumerator<'T>(stream) :> IEnumerator<'T>
                member __.GetEnumerator () = new StreamEnumerator<'T>(stream) :> System.Collections.IEnumerator
        }

    /// <summary>Creates an ResizeArray from the given stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result ResizeArray.</returns>    
    let inline toResizeArray (stream : Stream<'T>) : ResizeArray<'T> =
        (new List<'T>(), stream) ||> fold (fun acc value -> acc.Add(value); acc) 

    /// <summary>Creates an array from the given stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result array.</returns>    
    let toArray (stream : Stream<'T>) : 'T[] =
        let list = toResizeArray stream
        list.ToArray()

    /// <summary>Applies a key-generating function to each element of the input stream and yields a stream ordered by keys. </summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>  
    let inline sortBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (stream : Stream<'T>) : Stream<'T> =
        let values = new List<'T>()
        let keys = new List<'Key>()
        stream |> iter (fun value -> keys.Add(projection value); values.Add(value));
        let array = values.ToArray()
        Array.Sort(keys.ToArray(), array)
        array |> ofArray

    /// <summary>Locates the maximum element of the stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>The maximum item.</returns>  
    let inline maxBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (source : Stream<'T>) : 'T =
        let result =
            fold (fun state t -> 
                let key = projection t 
                match state with 
                | None -> Some (ref t, ref key)
                | Some (refValue, refKey) when !refKey < key -> 
                    refValue := t
                    refKey := key
                    state
                | _ -> state) None source

        match result with
        | None -> invalidArg "source" "The input sequence was empty."
        | Some (refValue, _) -> !refValue

    /// <summary>Locates the minimum element of the stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>The maximum item.</returns>  
    let inline minBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (source : Stream<'T>) : 'T =
        let result = 
            fold (fun state t ->
                let key = projection t 
                match state with 
                | None -> Some (ref t, ref key)
                | Some (refValue, refKey) when !refKey > key -> 
                    refValue := t
                    refKey := key
                    state
                | _ -> state) None source

        match result with
        | None -> invalidArg "source" "The input sequence was empty."
        | Some (refValue, _) -> !refValue

    /// <summary>Applies a state-updating function to a stream of inputs, grouped by key projection.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="folder">Folding function.</param>
    /// <param name="init">State initializing function.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline foldBy (projection : 'T -> 'Key) (folder : 'State -> 'T -> 'State) 
                      (init : unit -> 'State) (source : Stream<'T>) : Stream<'Key * 'State> =

        let dict = new Dictionary<'Key, 'State ref>()

        source |> iter (fun t -> 
            let key = projection t
            let mutable container = Unchecked.defaultof<'State ref>
            if not <| dict.TryGetValue(key, &container) then
                container <- ref <| init ()
                dict.Add(key, container)
            container := folder container.Value t)

        dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, keyValue.Value.Value))

    /// <summary>Applies a key-generating function to each element of the input stream and yields a stream of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline groupBy (projection : 'T -> 'Key) (source : Stream<'T>) : Stream<'Key * seq<'T>>  =
        let dict = new Dictionary<'Key, List<'T>>()
        
        source |> iter (fun t -> 
            let mutable grouping = Unchecked.defaultof<List<'T>>
            let key = projection t
            if not <| dict.TryGetValue(key, &grouping) then
                grouping <- new List<'T>()
                dict.Add(key, grouping)
            grouping.Add(t))
        dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, keyValue.Value :> seq<'T>))

    /// <summary>Applies a key-generating function to each element of the input stream and yields a stream of unique keys and their frequency.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline countBy (project : 'T -> 'Key) (stream : Stream<'T>) : Stream<'Key * int> =
        foldBy project (fun c _ -> c + 1) (fun () -> 0) stream

    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFind (predicate : 'T -> bool) (stream : Stream<'T>) : 'T option = 
        let resultRef = ref Unchecked.defaultof<'T option>
        stream |> Internals.iterCancel (fun cancel -> nocurry(); fun value -> if predicate value then resultRef := Some value; cancel())
        !resultRef

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the stream.</exception>
    let inline find (predicate : 'T -> bool) (stream : Stream<'T>) : 'T = 
        match tryFind predicate stream with
        | Some value -> value
        | None -> raise <| new KeyNotFoundException()

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPick (chooser : 'T -> 'R option) (stream : Stream<'T>) : 'R option = 
        let resultRef = ref Unchecked.defaultof<'R option>
        stream |> Internals.iterCancel (fun cancel -> nocurry(); fun value -> match chooser value with | Some value' -> resultRef := Some value'; cancel() | None -> ())
        !resultRef

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the stream evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the stream evaluates to None when the given function is applied.</exception>
    let inline pick (chooser : 'T -> 'R option) (stream : Stream<'T>) : 'R = 
        match tryPick chooser stream with
        | Some value' -> value'
        | None -> raise <| new KeyNotFoundException()

    /// <summary>Tests if any element of the stream satisfies the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline exists (predicate : 'T -> bool) (stream : Stream<'T>) : bool = 
        match tryFind predicate stream with
        | Some value -> true
        | None -> false

    /// <summary>Tests if all elements of the stream satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forall (predicate : 'T -> bool) (stream : Stream<'T>) : bool = 
        not <| exists (fun value -> not <| predicate value) stream


    /// <summary>
    ///     Separates stream elements into groups until predicate is satisfied.
    ///     Elements not satisfying the predicate are included as final element in grouping
    ///     or discarded depending on the 'inclusive' parameter.
    /// </summary>
    /// <param name="inclusive">Include elements not satisfying the predicate to the last grouping. Discarded otherwise.</param>
    /// <param name="predicate">Grouping predicate.</param>
    /// <param name="source">Source stream.</param>
    let groupUntil inclusive (predicate : 'T -> bool) (source : Stream<'T>) : Stream<'T []> =
        Stream (fun { Complete = complete; Cont = k; Cts = cts } ->
            let results = new ResizeArray<'T> ()
            source.Run 
                    { Complete = 
                        (fun () -> 
                            if results.Count > 0 then
                                k <| results.ToArray() 
                            complete ());
                      Cont = 
                        (fun (t : 'T) ->
                            if predicate t then
                                results.Add t
                                ()
                            else
                                if inclusive then results.Add t
                                let value = results.ToArray()
                                results.Clear()
                                k value);
                      Cts = cts })

    /// <summary>
    ///     Returns the first element of the stream.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element of the stream, or None if the stream has no elements.</returns>
    let tryHead (stream : Stream<'T>) : 'T option =
        let stream' = take 1 stream
        let resultRef = ref Unchecked.defaultof<'T option>
        stream' |> iter (fun value -> resultRef := Some value)
        !resultRef

    /// <summary>
    ///     Returns the first element of the stream.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element of the stream.</returns>
    /// <exception cref="System.ArgumentException">Thrown when the stream has no elements.</exception>
    let head (stream : Stream<'T>) : 'T =
        match tryHead stream with
        | Some value -> value
        | None -> invalidArg "stream" "The stream was empty."


    /// <summary>
    ///     Returns true if the stream is empty and false otherwise.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>true if the input stream is empty, false otherwise</returns>
    let isEmpty (stream : Stream<'T>) : bool =
        stream |> exists (fun _ -> true) |> not
