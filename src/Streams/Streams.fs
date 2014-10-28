namespace Nessos.Streams
open System
open System.Collections.Generic

/// Represents a Stream of values.
type Stream<'T> = Stream of (('T -> bool) -> unit)

/// Provides basic operations on Streams.
[<RequireQualifiedAccessAttribute>]
module Stream =
           
    /// <summary>Wraps array as a stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result stream.</returns>
    let inline ofArray (source : 'T []) : Stream<'T> =
        let iter iterf =
            let mutable i = 0
            let mutable next = true
            while i < source.Length && next do
                next <- iterf source.[i]
                i <- i + 1
        Stream iter

    /// <summary>Wraps ResizeArray as a stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result stream.</returns>
    let inline ofResizeArray (source : ResizeArray<'T>) : Stream<'T> =
        let iter iterf =
            let mutable i = 0
            let mutable next = true
            while i < source.Count && next do
                next <- iterf source.[i]
                i <- i + 1
        Stream iter

    /// <summary>Wraps seq as a stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result stream.</returns>
    let inline ofSeq (source : seq<'T>) : Stream<'T> =
        let iter iterf = 
            use enumerator = source.GetEnumerator()
            let mutable next = true
            while enumerator.MoveNext() && next do
                next <- iterf enumerator.Current
        Stream iter

    /// <summary>Transforms each element of the input stream.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter iterf =
            streamf (fun value -> iterf (f value))
        Stream iter

    /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline flatMap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter iterf =
            streamf (fun value -> 
                        let (Stream streamf') = f value;
                        streamf' iterf; true)
        Stream iter

    /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatMap f stream

    /// <summary>Filters the elements of the input stream.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline filter (predicate : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let iter iterf = 
            streamf (fun value -> if predicate value then iterf value else true)
        Stream iter

    /// <summary>Applies the given function to each element of the stream and returns the stream comprised of the results for each element where the function returns Some with some value.</summary>
    /// <param name="chooser">A function to transform items of type 'T into options of type 'R.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline choose (chooser : 'T -> 'R option) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter iterf = 
            streamf (fun value -> match chooser value with | Some value' -> iterf value' | None -> true)
        Stream iter

    /// <summary>Returns the elements of the stream up to a specified count.</summary>
    /// <param name="n">The number of items to take.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline take (n : int) (stream : Stream<'T>) : Stream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        let (Stream streamf) = stream
        let iter iterf = 
            let counter = ref 0
            streamf (fun value -> 
                incr counter
                if !counter <= n then iterf value else false)
        Stream iter

    /// <summary>Returns the elements of the stream while the given predicate returns true.</summary>
    /// <param name="pred">The predicate function.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline takeWhile pred (stream : Stream<'T>) : Stream<'T> = 
        let (Stream streamf) = stream
        let iter iterf = 
            streamf (fun value -> 
                if pred value then iterf value else false)
        Stream iter

    /// <summary>Returns a stream that skips N elements of the input stream and then yields the remaining elements of the stream.</summary>
    /// <param name="n">The number of items to skip.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    let inline skip (n : int) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let iter iterf = 
            let counter = ref 0
            streamf (fun value -> 
                incr counter
                if !counter > n then iterf value else true)
        Stream iter

    // terminal functions

    /// <summary>Applies a function to each element of the stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the stream.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : 'State =
        let (Stream streamf) = stream 
        let accRef = ref state
        streamf (fun value -> accRef := folder !accRef value ; true) 
        !accRef

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
    let inline length (stream : Stream<'T>) : int =
        fold (fun acc _  -> 1 + acc) 0 stream

    /// <summary>Applies the given function to each element of the stream.</summary>
    /// <param name="f">A function to apply to each element of the stream.</param>
    /// <param name="stream">The input stream.</param>    
    let inline iter (f : 'T -> unit) (stream : Stream<'T>) : unit = 
        let (Stream streamf) = stream
        streamf (fun value -> f value; true) 

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
        streamf (fun value -> keys.Add(projection value); values.Add(value); true)
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
                let k = projection t 
                match state with 
                | None -> Some (t, k)
                | Some (_, k') when k' < k -> Some (t, k) 
                | _ -> state) None source

        match result with
        | None -> invalidArg "source" "The input sequence was empty."
        | Some (t,_) -> t

    /// <summary>Locates the minimum element of the stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="source">The input stream.</param>
    /// <returns>The maximum item.</returns>  
    let inline minBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (source : Stream<'T>) : 'T =
        let result = 
            fold (fun state t ->
                let k = projection t 
                match state with 
                | None -> Some (t, k)
                | Some (_, k') when k' > k -> Some (t, k) 
                | _ -> state) None source

        match result with
        | None -> invalidArg "source" "The input sequence was empty."
        | Some (t,_) -> t

    /// <summary>Applies a state-updating function to a stream of inputs, grouped by key projection.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="folder">Folding function.</param>
    /// <param name="init">State initializing function.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline foldBy (projection : 'T -> 'Key) (folder : 'State -> 'T -> 'State) 
                        (init : unit -> 'State) (stream : Stream<'T>) : Stream<'Key * 'State> =

        let dict = new Dictionary<'Key, 'State ref>()

        let inline body (t : 'T) =
            let key = projection t
            let ok, state = dict.TryGetValue key
            let state =
                if ok then state
                else
                    let state = ref <| init ()
                    dict.Add(key, state)
                    state

            state := folder state.Value t

        do iter body stream
        dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, keyValue.Value.Value))

    /// <summary>Applies a key-generating function to each element of the input stream and yields a stream of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline groupBy (projection : 'T -> 'Key) (stream : Stream<'T>) : Stream<'Key * seq<'T>>  =
        let array = toArray stream
        let dict = new Dictionary<'Key, List<'T>>()
        let mutable grouping = Unchecked.defaultof<List<'T>>

        for i = 0 to array.Length - 1 do
            let key = projection array.[i]
            if not <| dict.TryGetValue(key, &grouping) then
                grouping <- new List<'T>()
                dict.Add(key, grouping)
            grouping.Add(array.[i])
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
        streamf (fun value -> if predicate value then resultRef := Some value; false; else true) 
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
        streamf (fun value -> match chooser value with | Some value' -> resultRef := Some value'; false; | None -> true) 
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