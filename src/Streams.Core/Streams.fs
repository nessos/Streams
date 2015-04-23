namespace Nessos.Streams
open System
open System.Collections.Generic
open System.Threading

/// Provides on-demand iteration 
type Iterator = {
    /// Function for on-demand processing
    TryAdvance : unit -> bool 
    /// Cleanup function
    Dispose : unit -> unit 
}

/// Provides functions for iteration
type Iterable = {
    /// Function for bulk processing
    Bulk : unit -> unit 
    /// Iterator for on-demand processing
    Iterator : Iterator
}

/// Represents the current executing contex
type Context<'T> = {
    /// The composed continutation
    Cont : 'T -> unit
    /// The completed continuation
    Complete : unit -> unit
    /// The current CancellationTokenSource
    Cts : CancellationTokenSource 
}

/// Represents a Stream of values.
type Stream<'T> = Stream of (Context<'T> -> Iterable) with
    override self.ToString() = 
        seq {
            use enumerator = new StreamEnumerator<'T>(self) :> IEnumerator<'T>
            while enumerator.MoveNext() do
                yield enumerator.Current
        } |> sprintf "%A"

// Wraps stream as a IEnumerable
and private StreamEnumerator<'T> (stream : Stream<'T>) =
    let results = new ResizeArray<'T>()
    let index = ref -1
    let count = ref 0
    let (Stream f) = stream
    let { Bulk = _; Iterator = { TryAdvance = tryAdvance; Dispose = dispose } } = 
        f { Complete = (fun () -> ()); 
            Cont =  (fun v -> 
                        let currentIndex = !count
                        incr count
                        if !count <= results.Count then
                            results.[currentIndex] <- v
                        else
                            results.Add(v); 
                        ()); Cts = null }

    interface System.Collections.IEnumerator with
        member __.Current = box results.[!index]
        member __.MoveNext () =
            let rec awaitNext () =
                incr index
                if !index >= !count then
                    count := 0
                    if tryAdvance () then
                        if !count > 0 then 
                            index := 0
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
        member __.Current = results.[!index]
        member __.Dispose () = dispose ()

/// Provides basic operations on Streams.
[<RequireQualifiedAccessAttribute>]
module Stream =

    /// <summary>The empty stream.</summary>
    /// <returns>An empty stream.</returns>
    let empty<'T> : Stream<'T> =
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            { Bulk = ignore; Iterator = { TryAdvance = (fun () -> false); Dispose = fun () -> if not (cts = null) then cts.Dispose() } }
        Stream iter

    /// <summary>Creates a singleton stream.</summary>
    /// <param name="source">The singleton stream element</param>
    /// <returns>A stream of just the given element</returns>
    let inline singleton (source: 'T) : Stream<'T> =
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            let bulk () = iterf source |> ignore; complete ()
            let tryAdvance =
                let pulled = ref false
                fun () ->
                    if !pulled then false
                    else
                        iterf source |> ignore
                        pulled := true
                        complete ()
                        true
            { Bulk = bulk; Iterator = { TryAdvance = tryAdvance; Dispose = fun () -> if not (cts = null) then cts.Dispose() } }
        Stream iter
           
    /// <summary>Wraps array as a stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result stream.</returns>
    let inline ofArray (source : 'T []) : Stream<'T> =
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            let bulk () =
                if not (cts = null) then
                    let mutable i = 0
                    let next = ref true 
                    cts.Token.Register(fun () -> next := false) |> ignore
                    use cts' = cts  
                    while i < source.Length && !next do
                        iterf source.[i]
                        i <- i + 1
                else
                    for i = 0 to source.Length - 1 do
                        iterf source.[i]
                complete ()
                
            let tryAdvance = 
                let i = ref 0
                let continueFlag = ref true
                if not (cts = null) then
                    cts.Token.Register(fun () -> continueFlag := false) |> ignore
                fun () -> 
                    if not !continueFlag then
                        false
                    else if !i < source.Length then
                        iterf source.[!i] 
                        if not !continueFlag then
                            complete ()
                        incr i
                        true
                    else
                        continueFlag := false
                        complete ()
                        true
            { Bulk = bulk; Iterator = { TryAdvance = tryAdvance; Dispose = fun () -> if not (cts = null) then cts.Dispose() } }
        Stream iter

    /// <summary>Wraps ResizeArray as a stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result stream.</returns>
    let inline ofResizeArray (source : ResizeArray<'T>) : Stream<'T> =
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            let bulk () =
                if not (cts = null) then
                    let mutable i = 0
                    let next = ref true 
                    cts.Token.Register(fun () -> next := false) |> ignore
                    use cts' = cts  
                    while i < source.Count && !next do
                        iterf source.[i]
                        i <- i + 1
                else
                    for i = 0 to source.Count - 1 do
                        iterf source.[i]
                complete ()

            let tryAdvance = 
                let i = ref 0
                let continueFlag = ref true
                if not (cts = null) then
                    cts.Token.Register(fun () -> continueFlag := false) |> ignore
                fun () -> 
                    if not !continueFlag then
                        false
                    else if !i < source.Count then
                        iterf source.[!i] 
                        if not !continueFlag then
                            complete ()
                        incr i
                        true
                    else
                        continueFlag := false
                        complete ()
                        true
            { Bulk = bulk; Iterator = { TryAdvance = tryAdvance; Dispose = fun () -> if not (cts = null) then cts.Dispose()  } }
        Stream iter

    /// <summary>Wraps seq as a stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result stream.</returns>
    let inline ofSeq (source : seq<'T>) : Stream<'T> =
        let iter { Complete = complete; Cont = iterf; Cts = cts } = 
            let bulk () =
                if not (cts = null) then
                    use enumerator = source.GetEnumerator()
                    let next = ref true
                    cts.Token.Register(fun () -> next := false) |> ignore
                    use cts' = cts
                    while enumerator.MoveNext() && !next do
                        iterf enumerator.Current
                    complete ()
                    enumerator.Dispose()
                else
                    for value in source do
                        iterf value
                    complete ()

            let iterator () = 
                let enumerator = source.GetEnumerator()
                let continueFlag = ref true
                if not (cts = null) then
                    cts.Token.Register(fun () -> continueFlag := false) |> ignore
                let tryAdvance () = 
                    if not !continueFlag then
                        false
                    else if enumerator.MoveNext() then
                        iterf enumerator.Current
                        if not !continueFlag then
                            complete ()
                        true
                    else
                        continueFlag := false
                        complete ()
                        true
                { TryAdvance = tryAdvance; Dispose = fun () -> if not (cts = null) then cts.Dispose() 
                                                               enumerator.Dispose() }
            { Bulk = bulk; Iterator = iterator () }
        match source with
        | :? ('T[]) as array -> ofArray array
        | :? ResizeArray<'T> as list -> ofResizeArray list
        | _ -> Stream iter
        
    /// <summary>Wraps an IEnumerable as a stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result stream.</returns>
    let inline cast<'T> (source : System.Collections.IEnumerable) : Stream<'T> =
        let iter { Complete = complete; Cont = iterf; Cts = cts } = 
            let bulk () =
                
                if not (cts = null) then
                    let enumerator = source.GetEnumerator() // not disposable
                    let next = ref true
                    cts.Token.Register(fun () -> next := false) |> ignore
                    use cts' = cts
                    while enumerator.MoveNext() && !next do
                        iterf (enumerator.Current :?> 'T)
                    complete ()
                    match enumerator with 
                    | :? System.IDisposable as disposable -> disposable.Dispose()
                    | _ -> ()                    
                else
                    for value in source do
                        iterf (value :?> 'T)
                    complete ()

            let iterator () = 
                let enumerator = source.GetEnumerator()
                let continueFlag = ref true
                if not (cts = null) then
                    cts.Token.Register(fun () -> continueFlag := false) |> ignore
                let tryAdvance () = 
                    if not !continueFlag || not <| enumerator.MoveNext()  then
                        match enumerator with 
                        | :? System.IDisposable as disposable -> disposable.Dispose()
                        | _ -> ()
                        false
                    else
                        iterf (enumerator.Current :?> 'T)
                        true
                let dispose () = 
                    match enumerator with 
                    | :? System.IDisposable as disposable -> disposable.Dispose()
                    | _ -> ()
                    complete ()
                    if not (cts = null) then cts.Dispose() 
                { TryAdvance = tryAdvance; Dispose = dispose }
            { Bulk = bulk; Iterator = iterator () }
        Stream iter

    /// <summary>Transforms each element of the input stream.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            streamf { Complete = complete; 
                      Cont = (fun value -> iterf (f value)); Cts = cts }
        Stream iter


    /// <summary>Transforms each element of the input stream. The integer index passed to the function indicates the index (from 0) of element being transformed.</summary>
    /// <param name="f">A function to transform items and also supplies the current index.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline mapi (f : int -> 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            let counter = ref -1
            streamf { Complete = complete; 
                      Cont = (fun value -> incr counter; iterf (f !counter value)); Cts = cts }
        Stream iter

    /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline flatMap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            streamf { Complete = complete;
                      Cont =  
                        (fun value -> 
                            let cts = 
                                if not (cts = null) then
                                    CancellationTokenSource.CreateLinkedTokenSource(cts.Token)
                                else cts
                            let (Stream streamf') = f value;
                            let { Bulk = bulk; Iterator = _ } = streamf' { Complete = (fun () -> ()); 
                                                                           Cont = iterf;
                                                                           Cts = cts } 
                                                                in bulk ());
                      Cts = cts }
        Stream iter

    /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatMap f stream

    /// <summary>Creates a cached version of the input stream.</summary>
    /// <param name="source">The input stream.</param>
    /// <returns>The cached stream.</returns>
    let inline cache (source: Stream<'T>): Stream<'T> =
        let cache = new ResizeArray<'T>()
        //if !cached = None && cache.Count = 0 then the stream is not cached
        //if !cached = None && cache.Count > 0 then the stream is partially cached
        //if !cached = Some then the stream is fully cached
        let cached = ref None : Stream<'T> option ref
        let iter { Complete = complete; Cont = iterf; Cts = cts } =  
            if Option.isSome !cached then
                //fully cached case
                let (Stream streamf) = (!cached).Value in streamf { Complete = complete; Cont = iterf; Cts = cts }
            else //partially cached or not cached at all case
                let (Stream streamf) = source
                let count = ref 0
                let { Bulk = bulk; Iterator = { TryAdvance = tryAdvance; Dispose = dispose } } = 
                        streamf { Complete = complete;
                                  Cont = (fun v -> (if cache.Count - !count = 0 then cache.Add(v)); incr count; iterf v);
                                  Cts = cts }
                let bulk' () = lock cache (fun () -> bulk(); cached := Some (ofResizeArray cache))

                //locking each next() seem's overkill
                let tryAdvance' () = lock cache (fun () -> if tryAdvance() then true else cached := Some (ofResizeArray cache); false)

                { Bulk = bulk'; Iterator = { TryAdvance = tryAdvance'; Dispose = dispose } }
        
        Stream iter



    /// <summary>Filters the elements of the input stream.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline filter (predicate : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let iter { Complete = complete; Cont = iterf; Cts = cts }  = 
            streamf { Complete = complete;
                      Cont = (fun value -> if predicate value then iterf value else ());
                      Cts = cts }
        Stream iter

    /// <summary>Applies the given function to each element of the stream and returns the stream comprised of the results for each element where the function returns Some with some value.</summary>
    /// <param name="chooser">A function to transform items of type 'T into options of type 'R.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline choose (chooser : 'T -> 'R option) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter { Complete = complete; Cont = iterf; Cts = cts }  = 
            streamf { Complete = complete; 
                      Cont = (fun value -> match chooser value with | Some value' -> iterf value' | None -> ());
                      Cts = cts }
        Stream iter

    /// <summary>Returns the elements of the stream up to a specified count.</summary>
    /// <param name="n">The number of items to take.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline take (n : int) (stream : Stream<'T>) : Stream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        let (Stream streamf) = stream
        let iter { Complete = complete; Cont = iterf; Cts = cts } = 
            let counter = ref 0
            let cts = if cts = null then new CancellationTokenSource() else cts
            streamf { Complete = complete;
                      Cont = 
                        (fun value -> 
                            incr counter
                            if !counter < n then 
                                iterf value
                            else if !counter = n then
                                iterf value
                                cts.Cancel());
                      Cts = cts } 
        Stream iter

    /// <summary>Returns the elements of the stream while the given predicate returns true.</summary>
    /// <param name="pred">The predicate function.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline takeWhile pred (stream : Stream<'T>) : Stream<'T> = 
        let (Stream streamf) = stream
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            let cts = if cts = null then new CancellationTokenSource() else cts 
            streamf { Complete = complete;
                      Cont = 
                        (fun value -> 
                            if pred value then iterf value else cts.Cancel());
                      Cts = cts }
        Stream iter

    /// <summary>Returns a stream that skips N elements of the input stream and then yields the remaining elements of the stream.</summary>
    /// <param name="n">The number of items to skip.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline skip (n : int) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let iter { Complete = complete; Cont = iterf; Cts = cts } = 
            let counter = ref 0
            streamf { Complete = complete;
                      Cont = 
                        (fun value -> 
                            incr counter
                            if !counter > n then iterf value else ());
                      Cts = cts } 
        Stream iter


    /// <summary>Concatenates a collection of streams.</summary>
    /// <param name="streams">The sequence of streams to concatenate.</param>
    /// <returns>The concatenated stream.</returns>
    let concat (streams: #seq<Stream<'T>>): Stream<'T> =
        let iter { Complete = complete; Cont = iterf; Cts = cts } =
            let bulk () =
                for stream in streams do
                    let (Stream streamF) = stream
                    let { Bulk = bulk; Iterator = _ } = streamF { Complete = (fun () -> ());
                                                                  Cont = iterf;
                                                                  Cts = cts }
                    bulk ()
                complete ()
            let iterator =
                if Seq.isEmpty streams then { TryAdvance = (fun () -> false); Dispose = fun () -> () }
                else                    
                    let enumerator =
                        let streams = 
                            streams
                            |> Seq.collect (fun stream ->         
                                             { new IEnumerable<'T> with
                                                member __.GetEnumerator () = new StreamEnumerator<'T>(stream) :> IEnumerator<'T>
                                                member __.GetEnumerator () = new StreamEnumerator<'T>(stream) :> System.Collections.IEnumerator })
                        streams.GetEnumerator()
                    let continueFlag = ref true
                    if not (cts = null) then
                        cts.Token.Register(fun _ -> continueFlag := false) |> ignore
                    let tryAdvance () =
                        if not !continueFlag then
                            false
                        else if enumerator.MoveNext() then
                            iterf enumerator.Current 
                            if not !continueFlag then
                                complete ()
                            true
                        else 
                            continueFlag := false
                            complete ()
                            true
                    let dispose () =
                        if not (cts = null) then cts.Dispose()
                        enumerator.Dispose()
                    { TryAdvance = tryAdvance; Dispose = dispose }

            { Bulk = bulk; Iterator = iterator }

        Stream iter


    /// <summary>Applies a specified function to the corresponding elements of two streams, producing a stream of the results.</summary>
    /// <param name="f">The combiner function.</param>
    /// <param name="first">The first input stream.</param>
    /// <param name="second">The second input stream.</param>
    /// <returns>The result stream.</returns>
    let zipWith (f : 'T -> 'S -> 'R) (first : Stream<'T>) (second : Stream<'S>) : Stream<'R> =
        let iter { Complete = complete; Cont = iterf; Cts = cts }  =
            let bulk () =
                let firstEnumerator = new StreamEnumerator<'T>(first) :> IEnumerator<'T>
                let secondEnumerator = new StreamEnumerator<'S>(second) :> IEnumerator<'S>
                let next = ref true
                if not (cts = null) then
                    cts.Token.Register(fun _ -> next := false) |> ignore
                use cts' = cts
                while !next do
                    if firstEnumerator.MoveNext() && secondEnumerator.MoveNext() then
                        iterf (f firstEnumerator.Current secondEnumerator.Current)
                    else
                        next := false
                    ()
                complete ()
            let iterator = 
                let continueFlag = ref true
                if not (cts = null) then
                    cts.Token.Register(fun _ -> continueFlag := false) |> ignore
                let firstEnumerator = new StreamEnumerator<'T>(first) :> IEnumerator<'T>
                let secondEnumerator = new StreamEnumerator<'S>(second) :> IEnumerator<'S>
                let tryAdvance () = 
                    if not !continueFlag then
                        false
                    else if firstEnumerator.MoveNext() && secondEnumerator.MoveNext() then
                        iterf (f firstEnumerator.Current secondEnumerator.Current)
                        if not !continueFlag then
                            complete ()
                        true
                    else
                        continueFlag := false
                        complete ()
                        true
                let dispose () = 
                    if not (cts = null) then
                        cts.Dispose()
                    firstEnumerator.Dispose()
                    secondEnumerator.Dispose()
                { TryAdvance = tryAdvance; Dispose = dispose }
            { Bulk = bulk; Iterator = iterator }
        Stream iter


    /// <summary>Applies a function to each element of the stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the stream.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : 'State =
        let (Stream streamf) = stream 
        let accRef = ref state
        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ());
                                                      Cont = (fun value -> accRef := folder !accRef value);
                                                      Cts = null }

        bulk ()
        !accRef

    /// <summary>Like Stream.fold, but computes on-demand and returns the stream of intermediate and final results</summary>
    /// <param name="folder">A function that updates the state with each element from the stream.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The final stream.</returns>
    let inline scan (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : Stream<'State> =
        let (Stream streamf) = stream 
        let iter { Complete = complete; Cont = iterf; Cts = cts } = 
            let accRef = ref state
            iterf !accRef
            streamf { Complete = complete;
                      Cont = (fun value -> accRef := folder !accRef value; iterf !accRef);
                      Cts = cts }

        Stream iter

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (stream : Stream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) LanguagePrimitives.GenericZero stream


    /// <summary>
    ///    Reduces the elements of the input stream to a single value via the given reducer function.
    ///    The reducer function is first applied to the first two elements of the stream.
    ///    Then, the reducer is applied on the result of the first reduction and the third element.
    //     The process continues until all the elements of the stream have been reduced.
    /// </summary>
    /// <param name="reducer">The reducer function.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The reduced value.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input stream is empty.</exception>
    let inline reduce (reducer : 'T -> 'T -> 'T) (stream : Stream<'T>) : 'T =
        let result =
            fold (fun state x ->
                      match state with
                      | None -> Some (ref x)
                      | Some y -> y := reducer !y x; state
                 ) None stream

        match result with
        | None -> invalidArg "stream" "The input stream was empty."
        | Some y -> !y

    /// <summary>Returns the total number of elements of the stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The total number of elements.</returns>
    let inline length (stream : Stream<'T>) : int =
        fold (fun acc _  -> 1 + acc) 0 stream

    /// <summary>Applies the given function to each element of the stream.</summary>
    /// <param name="f">A function to apply to each element of the stream.</param>
    /// <param name="stream">The input stream.</param>    
    let inline iter (f : 'T -> unit) (stream : Stream<'T>) : unit = 
        let (Stream streamf) = stream
        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ());
                                                      Cont = (fun value -> f value);
                                                      Cts = null } 
        bulk ()

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
        let (Stream _) = stream
        let list = 
            fold (fun (acc : List<'T>) value -> acc.Add(value); acc) (new List<'T>()) stream 
        list

    /// <summary>Creates an array from the given stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result array.</returns>    
    let inline toArray (stream : Stream<'T>) : 'T[] =
        let list = toResizeArray stream
        list.ToArray()

    /// <summary>Applies a key-generating function to each element of the input stream and yields a stream ordered by keys. </summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>  
    let inline sortBy<'T, 'Key when 'Key :> IComparable<'Key>> (projection : 'T -> 'Key) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let values = new List<'T>()
        let keys = new List<'Key>()
        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ());
                                                      Cont = (fun value -> keys.Add(projection value); values.Add(value));
                                                      Cts = null }
        bulk ()
        let array = values.ToArray()
        Array.Sort(keys.ToArray(), array)
        array |> ofArray

    /// <summary>Locates the maximum element of the stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>The maximum item.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input stream is empty.</exception>
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
        | None -> invalidArg "source" "The input stream was empty."
        | Some (refValue, _) -> !refValue

    /// <summary>Locates the minimum element of the stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>The maximum item.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input stream is empty.</exception>
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
        | None -> invalidArg "source" "The input stream was empty."
        | Some (refValue, _) -> !refValue

    /// <summary>Computes the average of the projections given by the supplied function on the input stream.</summary>
    /// <param name="projection">A function to transform items of the input stream into a projection.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>The computed average.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input stream is empty.</exception>
    let inline averageBy (projection : 'T -> ^U) (source : Stream<'T>) : ^U
            when ^U : (static member (+) : ^U * ^U -> ^U)
            and  ^U : (static member DivideByInt : ^U * int -> ^U)
            and  ^U : (static member Zero : ^U) =
        let (y, c) =
            fold (fun ((y, c) as state) v ->
                      y := Checked.(+) !y (projection v)
                      incr c
                      state)
                 (ref LanguagePrimitives.GenericZero, ref 0)
                 source

        if !c = 0 then invalidArg "source" "The input stream was empty."
        else LanguagePrimitives.DivideByInt !y !c

    /// <summary>Computes the average of the elements in the input stream.</summary>
    /// <param name="source">The input stream.</param>
    /// <returns>The computed average.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input stream is empty.</exception>
    let inline average (source : Stream< ^T >) : ^T
            when ^T : (static member (+) : ^T * ^T -> ^T)
            and  ^T : (static member DivideByInt : ^T * int -> ^T)
            and  ^T : (static member Zero : ^T) =
        averageBy id source

    /// <summary>Applies a state-updating function to a stream of inputs, grouped by key projection.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="folder">Folding function.</param>
    /// <param name="init">State initializing function.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline foldBy (projection : 'T -> 'Key) (folder : 'State -> 'T -> 'State) 
                        (init : unit -> 'State) (source : Stream<'T>) : Stream<'Key * 'State> =

        let dict = new Dictionary<'Key, 'State ref>()

        let inline body (t : 'T) =
            let key = projection t
            let mutable container = Unchecked.defaultof<'State ref>
            if not <| dict.TryGetValue(key, &container) then
                container <- ref <| init ()
                dict.Add(key, container)

            container := folder container.Value t
            ()

        let (Stream iter) = source in let { Bulk = bulk; Iterator = _ } =
                                                iter { Complete = (fun () -> ());
                                                Cont = body; Cts = null }; in bulk ()
        dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, keyValue.Value.Value))

    /// <summary>Applies a key-generating function to each element of the input stream and yields a stream of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline groupBy (projection : 'T -> 'Key) (source : Stream<'T>) : Stream<'Key * seq<'T>>  =
        let dict = new Dictionary<'Key, List<'T>>()
        
        let inline body (t : 'T) = 
            let mutable grouping = Unchecked.defaultof<List<'T>>
            let key = projection t
            if not <| dict.TryGetValue(key, &grouping) then
                grouping <- new List<'T>()
                dict.Add(key, grouping)
            grouping.Add(t)
            ()

        let (Stream iterf) = source in let { Bulk = bulk; Iterator = _ } = 
                                        iterf { Complete = (fun () -> ());
                                                Cont = body; Cts = null } in bulk ()
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
        let (Stream streamf) = stream
        let resultRef = ref Unchecked.defaultof<'T option>
        let cts = new CancellationTokenSource()
        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ());
                                                      Cont = (fun value -> if predicate value then resultRef := Some value; cts.Cancel(); else ());
                                                      Cts = cts } 
        bulk ()
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
        let (Stream streamf) = stream
        let resultRef = ref Unchecked.defaultof<'R option>
        let cts = new CancellationTokenSource()
        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ());
                                                      Cont = (fun value -> match chooser value with | Some value' -> resultRef := Some value'; cts.Cancel(); | None -> ())
                                                      Cts = cts } 
        bulk ()
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
        let iter { Complete = complete; Cont = k; Cts = cts } =
            let results = new ResizeArray<'T> ()
            let (Stream streamf) = source
            streamf { Complete = 
                        (fun () -> 
                            if results.Count > 0 then
                                k <| results.ToArray() |> ignore
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
                      Cts = cts }

        Stream iter

    /// <summary>
    ///     Returs the first element of the stream.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element of the stream, or None if the stream has no elements.</returns>
    let inline tryHead (stream : Stream<'T>) : 'T option =
        let stream' = take 1 stream
        let (Stream streamf) = stream'
        let resultRef = ref Unchecked.defaultof<'T option>
        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ());
                                                      Cont = (fun value -> resultRef := Some value);
                                                      Cts = null }

        bulk ()
        !resultRef

    /// <summary>
    ///     Returs the first element of the stream.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element of the stream.</returns>
    /// <exception cref="System.ArgumentException">Thrown when the stream has no elements.</exception>
    let inline head (stream : Stream<'T>) : 'T =
        match tryHead stream with
        | Some value -> value
        | None -> invalidArg "stream" "The input stream was empty."


    /// <summary>
    ///     Returs true if the stream is empty and false otherwise.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>true if the input stream is empty, false otherwise</returns>
    let inline isEmpty (stream : Stream<'T>) : bool =
        stream |> exists (fun _ -> true) |> not
