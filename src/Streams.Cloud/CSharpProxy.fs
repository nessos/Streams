namespace Nessos.Streams.Internals.Cloud
open System
open System.Collections
open System.Collections.Generic
open Nessos.Streams
open Nessos.Streams.Cloud

/// [omit]
/// Proxy for FSharp type specialization and lambda inlining.
type public CSharpProxy = 

    static member Select<'T, 'R> (stream : CloudStream<'T>, func : Func<'T, 'R>) = 
        CloudStream.map (fun x -> func.Invoke(x)) stream

    static member Where<'T> (stream : CloudStream<'T>, func : Func<'T, bool>) = 
        CloudStream.filter (fun x -> func.Invoke(x)) stream

    static member SelectMany<'T, 'R>(stream : CloudStream<'T>, func : Func<'T, Stream<'R>>) =
        CloudStream.flatMap (fun x -> func.Invoke(x)) stream 

    static member Aggregate<'T, 'Acc>(stream : CloudStream<'T>, state : Func<'Acc>, folder : Func<'Acc, 'T, 'Acc>, combiner : Func<'Acc, 'Acc, 'Acc>) = 
        CloudStream.fold (fun acc x -> folder.Invoke(acc, x)) (fun left right -> combiner.Invoke(left, right)) (fun _ -> state.Invoke()) stream

    static member AggregateBy<'T, 'Key, 'Acc when 'Key : equality>(stream : CloudStream<'T>, projection : Func<'T,'Key> , state : Func<'Acc>, folder : Func<'Acc, 'T, 'Acc>, combiner : Func<'Acc, 'Acc, 'Acc>) = 
        CloudStream.foldBy (fun x -> projection.Invoke(x)) (fun acc x -> folder.Invoke(acc, x)) (fun left right -> combiner.Invoke(left, right)) (fun _ -> state.Invoke()) stream

    static member OrderBy<'T, 'Key when 'Key :> IComparable<'Key>>(stream : CloudStream<'T>, func : Func<'T, 'Key>, takeCount : int) =
        CloudStream.sortBy (fun x -> func.Invoke(x)) takeCount stream

    static member Count<'T>(stream : CloudStream<'T>) = 
        CloudStream.length stream
        
    static member CountBy<'T, 'Key when 'Key : equality>(stream : CloudStream<'T>, func : Func<'T, 'Key>) =
        CloudStream.countBy (fun x -> func.Invoke(x)) stream

    static member Sum(stream : CloudStream<int64>) = 
        CloudStream.sum stream

    static member Sum(stream : CloudStream<int>) = 
        CloudStream.sum stream

    static member Sum(stream : CloudStream<single>) = 
        CloudStream.sum stream

    static member Sum(stream : CloudStream<double>) = 
        CloudStream.sum stream

    static member Sum(stream : CloudStream<decimal>) = 
        CloudStream.sum stream
