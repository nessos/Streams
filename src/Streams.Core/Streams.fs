namespace Nessos.Streams.Core
open System
open System.Collections.Generic
open System.Linq


type Stream<'T> = Stream of (('T -> bool) -> unit)
module Stream =
           
    // generator functions
    let inline ofArray (source : 'T []) : Stream<'T> =
        let iter iterf =
            let mutable i = 0
            let mutable next = true
            while i < source.Length && next do
                next <- iterf source.[i]
                i <- i + 1
        Stream iter

    let inline ofResizeArray (source : ResizeArray<'T>) : Stream<'T> =
        let iter iterf =
            let mutable i = 0
            let mutable next = true
            while i < source.Count && next do
                next <- iterf source.[i]
                i <- i + 1
        Stream iter

    let inline ofSeq (source : seq<'T>) : Stream<'T> =
        let iter iterf = 
            use enumerator = source.GetEnumerator()
            let mutable next = true
            while enumerator.MoveNext() && next do
                next <- iterf enumerator.Current
        Stream iter

    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter iterf =
            streamf (fun value -> iterf (f value))
        Stream iter

    let inline flatMap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter iterf =
            streamf (fun value -> 
                        let (Stream streamf') = f value;
                        streamf' iterf; true)
        Stream iter

    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatMap f stream

    let inline filter (predicate : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let iter iterf = 
            streamf (fun value -> if predicate value then iterf value else true)
        Stream iter

    let inline choose (chooser : 'T -> 'R option) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter iterf = 
            streamf (fun value -> match chooser value with | Some value' -> iterf value' | None -> true)
        Stream iter

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

    let inline takeWhile pred (stream : Stream<'T>) : Stream<'T> = 
        let (Stream streamf) = stream
        let iter iterf = 
            streamf (fun value -> 
                if pred value then iterf value else false)
        Stream iter

    let inline skip (n : int) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let iter iterf = 
            let counter = ref 0
            streamf (fun value -> 
                incr counter
                if !counter > n then iterf value else true)
        Stream iter

    // terminal functions
    let inline fold (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : 'State =
        let (Stream streamf) = stream 
        let accRef = ref state
        streamf (fun value -> accRef := folder !accRef value ; true) 
        !accRef

    let inline sum (stream : Stream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) LanguagePrimitives.GenericZero stream

    let inline length (stream : Stream<'T>) : int =
        fold (fun acc _  -> 1 + acc) 0 stream

    let inline iter (f : 'T -> unit) (stream : Stream<'T>) : unit = 
        let (Stream streamf) = stream
        streamf (fun value -> f value; true) 

    let inline toResizeArray (stream : Stream<'T>) : ResizeArray<'T> =
        let (Stream _) = stream
        let list = 
            fold (fun (acc : List<'T>) value -> acc.Add(value); acc) (new List<'T>()) stream 
        list

    let inline toArray (stream : Stream<'T>) : 'T[] =
        let list = toResizeArray stream
        list.ToArray()

    let inline sortBy<'T, 'Key when 'Key :> IComparable<'Key>> (projection : 'T -> 'Key) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let values = new List<'T>()
        let keys = new List<'Key>()
        streamf (fun value -> keys.Add(projection value); values.Add(value); true)
        let array = values.ToArray()
        Array.Sort(keys.ToArray(), array)
        array |> ofArray

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

    let inline tryFind (predicate : 'T -> bool) (stream : Stream<'T>) : 'T option = 
        let (Stream streamf) = stream
        let resultRef = ref Unchecked.defaultof<'T option>
        streamf (fun value -> if predicate value then resultRef := Some value; false; else true) 
        !resultRef


    let inline find (predicate : 'T -> bool) (stream : Stream<'T>) : 'T = 
        match tryFind predicate stream with
        | Some value -> value
        | None -> raise <| new KeyNotFoundException()

    let inline tryPick (chooser : 'T -> 'R option) (stream : Stream<'T>) : 'R option = 
        let (Stream streamf) = stream
        let resultRef = ref Unchecked.defaultof<'R option>
        streamf (fun value -> match chooser value with | Some value' -> resultRef := Some value'; false; | None -> true) 
        !resultRef

    let inline pick (chooser : 'T -> 'R option) (stream : Stream<'T>) : 'R = 
        match tryPick chooser stream with
        | Some value' -> value'
        | None -> raise <| new KeyNotFoundException()

    let inline exists (predicate : 'T -> bool) (stream : Stream<'T>) : bool = 
        match tryFind predicate stream with
        | Some value -> true
        | None -> false

    let inline forall (predicate : 'T -> bool) (stream : Stream<'T>) : bool = 
        not <| exists (fun value -> not <| predicate value) stream