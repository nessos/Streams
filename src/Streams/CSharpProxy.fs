namespace Nessos.Streams.Internals
open System
open System.Collections
open System.Collections.Generic
open Nessos.Streams

/// [omit]
/// Proxy for FSharp type specialization and lambda inlining.
type public CSharpProxy = 

    
    static member Select<'T, 'R> (stream : Stream<'T>, func : Func<'T, 'R>) = 
        Stream.map (fun x -> func.Invoke(x)) stream

    static member Select<'T, 'R> (stream : ParStream<'T>, func : Func<'T, 'R>) = 
        ParStream.map (fun x -> func.Invoke(x)) stream


    static member Where<'T> (stream : Stream<'T>, func : Func<'T, bool>) = 
        Stream.filter (fun x -> func.Invoke(x)) stream

    static member Where<'T> (stream : ParStream<'T>, func : Func<'T, bool>) = 
        ParStream.filter (fun x -> func.Invoke(x)) stream

    static member SelectMany<'T, 'R>(stream : Stream<'T>, func : Func<'T, Stream<'R>>) =
        Stream.flatMap (fun x -> func.Invoke(x)) stream 

    static member SelectMany<'T, 'R>(stream : ParStream<'T>, func : Func<'T, Stream<'R>>) =
        ParStream.flatMap (fun x -> func.Invoke(x)) stream 

    static member Aggregate<'T, 'Acc>(stream : Stream<'T>, state : 'Acc, folder : Func<'Acc, 'T, 'Acc>) = 
        Stream.fold (fun acc x -> folder.Invoke(acc, x)) state stream

    static member Aggregate<'T, 'Acc>(stream : ParStream<'T>, state : Func<'Acc>, folder : Func<'Acc, 'T, 'Acc>, combiner : Func<'Acc, 'Acc, 'Acc>) = 
        ParStream.fold (fun acc x -> folder.Invoke(acc, x)) (fun left right -> combiner.Invoke(left, right)) (fun _ -> state.Invoke()) stream


    static member GroupBy<'T, 'Key when 'Key : equality>(stream : Stream<'T>, func : Func<'T, 'Key>) =
        stream
        |> Stream.groupBy (fun x -> func.Invoke(x)) 
        |> Stream.map (fun (key, values) -> 
            { new System.Linq.IGrouping<'Key, 'T> with
                    member self.Key = key
                    member self.GetEnumerator() : IEnumerator<'T> = values.GetEnumerator()
                interface System.Collections.IEnumerable with
                    member self.GetEnumerator() : IEnumerator = values.GetEnumerator() :> _ }) 

    static member GroupBy<'T, 'Key when 'Key : equality>(stream : ParStream<'T>, func : Func<'T, 'Key>) =
        stream
        |> ParStream.groupBy (fun x -> func.Invoke(x))
        |> ParStream.map (fun (key, values) -> 
            { new System.Linq.IGrouping<'Key, 'T> with
                    member self.Key = key
                    member self.GetEnumerator() : IEnumerator<'T> = values.GetEnumerator()
                interface System.Collections.IEnumerable with
                    member self.GetEnumerator() : IEnumerator = values.GetEnumerator() :> _ }) 

    static member OrderBy<'T, 'Key when 'Key : comparison>(stream : Stream<'T>, func : Func<'T, 'Key>) =
        Stream.sortBy (fun x -> func.Invoke(x)) stream

    static member OrderBy<'T, 'Key when 'Key : comparison>(stream : ParStream<'T>, func : Func<'T, 'Key>) =
        ParStream.sortBy (fun x -> func.Invoke(x)) stream

    static member OrderByDescending<'T, 'Key when 'Key : comparison>(stream : ParStream<'T>, func : Func<'T, 'Key>) =
        ParStream.sortByDescending (fun x -> func.Invoke(x)) stream

    static member OrderBy<'T, 'Key>(stream : ParStream<'T>, func : Func<'T, 'Key>, comparer : IComparer<'Key>) =
        ParStream.sortByUsing (fun x -> func.Invoke(x)) comparer stream 

    static member Sum(stream : Stream<int64>) = 
        Stream.sum stream

    static member Sum(stream : ParStream<int64>) = 
        ParStream.sum stream

    static member Sum(stream : Stream<int>) = 
        Stream.sum stream

    static member Sum(stream : ParStream<int>) = 
        ParStream.sum stream

    static member Sum(stream : Stream<single>) = 
        Stream.sum stream

    static member Sum(stream : ParStream<single>) = 
        ParStream.sum stream

    static member Sum(stream : Stream<double>) = 
        Stream.sum stream

    static member Sum(stream : ParStream<double>) = 
        ParStream.sum stream

    static member Sum(stream : Stream<decimal>) = 
        Stream.sum stream

    static member Sum(stream : ParStream<decimal>) = 
        ParStream.sum stream


    static member Count<'T>(stream : Stream<'T>) = 
        Stream.length stream

    static member Count<'T>(stream : ParStream<'T>) = 
        ParStream.length stream

    static member First<'T>(stream : Stream<'T>, func : Func<'T, bool>)  = 
        match Stream.tryFind (fun x -> func.Invoke(x)) stream with
        | Some value -> value
        | None -> invalidOp "Stream is empty or no elements satisfy the predicate."

    static member First<'T>(stream : Stream<'T>) =
        match Stream.tryHead stream with
        | Some value -> value
        | None -> invalidOp "Stream is empty"

    static member FirstOrDefault<'T>(stream : Stream<'T>) =
        match Stream.tryHead stream with
        | Some value -> value
        | None -> Unchecked.defaultof<'T>

    static member First<'T>(stream : ParStream<'T>, func : Func<'T, bool>)  = 
        match ParStream.tryFind (fun x -> func.Invoke(x)) stream with
        | Some value -> value
        | None -> invalidOp "Stream is empty or no elements satisfy the predicate."


    static member First<'T>(stream : ParStream<'T>) =
        match ParStream.tryHead stream with
        | Some value -> value
        | None -> invalidOp "Stream is empty"

    static member FirstOrDefault<'T>(stream : ParStream<'T>) =
        match ParStream.tryHead stream with
        | Some value -> value
        | None -> Unchecked.defaultof<'T>

    static member Any<'T>(stream : Stream<'T>, func : Func<'T, bool>)  = 
        Stream.exists (fun x -> func.Invoke(x)) stream

    static member Any<'T>(stream : ParStream<'T>, func : Func<'T, bool>)  = 
        ParStream.exists (fun x -> func.Invoke(x)) stream

    static member All<'T>(stream : Stream<'T>, func : Func<'T, bool>)  = 
        Stream.forall (fun x -> func.Invoke(x)) stream

    static member All<'T>(stream : ParStream<'T>, func : Func<'T, bool>)  = 
        ParStream.forall (fun x -> func.Invoke(x)) stream


    static member MaxBy<'T,'U when 'U : comparison>(stream : Stream<'T>, func : Func<'T,'U>) =
        Stream.maxBy (fun x -> func.Invoke(x)) stream

    static member MaxBy<'T,'U when 'U : comparison>(stream : ParStream<'T>, func : Func<'T,'U>) =
        ParStream.maxBy (fun x -> func.Invoke(x)) stream

    static member MinBy<'T,'U when 'U : comparison>(stream : Stream<'T>, func : Func<'T,'U>) =
        Stream.minBy (fun x -> func.Invoke(x)) stream

    static member MinBy<'T,'U when 'U : comparison>(stream : ParStream<'T>, func : Func<'T,'U>) =
        ParStream.minBy (fun x -> func.Invoke(x)) stream

    static member CountBy<'T,'U when 'U : equality>(stream : Stream<'T>, func : Func<'T,'U>) =
        Stream.countBy (fun x -> func.Invoke(x)) stream

    static member CountBy<'T,'U when 'U : equality>(stream : ParStream<'T>, func : Func<'T,'U>) =
        ParStream.countBy (fun x -> func.Invoke(x)) stream

    static member AggregateBy<'T,'Key, 'State when 'Key : equality>(stream : Stream<'T>, proj : Func<'T,'Key>, folder : Func<'State,'T,'State>, init : Func<'State>) =
        Stream.foldBy (fun x -> proj.Invoke(x)) (fun x s -> folder.Invoke(x,s)) (fun () -> init.Invoke()) stream

    static member AggregateBy<'T,'Key, 'State when 'Key : equality>(stream : ParStream<'T>, proj : Func<'T,'Key>, folder : Func<'State,'T,'State>, init : Func<'State>) =
        ParStream.foldBy (fun x -> proj.Invoke(x)) (fun x s -> folder.Invoke(x,s)) (fun () -> init.Invoke()) stream

    static member Zip<'TFirst, 'TSecond, 'TResult>(first : Stream<'TFirst>, second : Stream<'TSecond>, resultSelector : Func<'TFirst, 'TSecond, 'TResult>) = 
        Stream.zipWith (fun x y -> resultSelector.Invoke(x, y)) first second


    static member IsEmpty<'T>(source : Stream<'T>) : bool = Stream.isEmpty source

    static member IsEmpty<'T>(source : ParStream<'T>) : bool = ParStream.isEmpty source

//    static member Skip<'T>(stream : Stream<'T>, count : int) =
//        Stream.skip count stream
//
//    static member Skip<'T>(stream : ParStream<'T>, count : int) =
//        ParStream.skip count stream
//    
//    static member Take<'T>(stream : Stream<'T>, count : int) =
//        Stream.skip count stream
//
//    static member Take<'T>(stream : ParStream<'T>, count : int) =
//        ParStream.skip count stream    
