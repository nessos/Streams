namespace Nessos.Streams.Core
open System
open System.Runtime.CompilerServices

// Proxy for FSharp type specialization and lambda inlining
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
        Stream.groupBy (fun x -> func.Invoke(x)) stream

    static member GroupBy<'T, 'Key when 'Key : equality>(stream : ParStream<'T>, func : Func<'T, 'Key>) =
        ParStream.groupBy (fun x -> func.Invoke(x)) stream

    static member OrderBy<'T, 'Key when 'Key :> IComparable<'Key>>(stream : Stream<'T>, func : Func<'T, 'Key>) =
        Stream.sortBy (fun x -> func.Invoke(x)) stream

    static member OrderBy<'T, 'Key when 'Key :> IComparable<'Key>>(stream : ParStream<'T>, func : Func<'T, 'Key>) =
        ParStream.sortBy (fun x -> func.Invoke(x)) stream

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
        Stream.find (fun x -> func.Invoke(x)) stream

    static member First<'T>(stream : ParStream<'T>, func : Func<'T, bool>)  = 
        ParStream.find (fun x -> func.Invoke(x)) stream

    static member Any<'T>(stream : Stream<'T>, func : Func<'T, bool>)  = 
        Stream.exists (fun x -> func.Invoke(x)) stream

    static member Any<'T>(stream : ParStream<'T>, func : Func<'T, bool>)  = 
        ParStream.exists (fun x -> func.Invoke(x)) stream

    static member All<'T>(stream : Stream<'T>, func : Func<'T, bool>)  = 
        Stream.forall (fun x -> func.Invoke(x)) stream

    static member All<'T>(stream : ParStream<'T>, func : Func<'T, bool>)  = 
        ParStream.forall (fun x -> func.Invoke(x)) stream


    
    


