namespace Nessos.Streams.Core
open System
open System.Runtime.CompilerServices

// Proxy for FSharp type specialization and lambda inlining
type public CSharpProxy = 

    
    static member Select<'T, 'R> (stream : Stream<'T>, func :System.Func<'T, 'R>) = 
        Stream.map (fun x -> func.Invoke(x)) stream

    static member Select<'T, 'R> (stream : ParStream<'T>, func :System.Func<'T, 'R>) = 
        ParStream.map (fun x -> func.Invoke(x)) stream


    static member Where<'T> (stream : Stream<'T>, func :System.Func<'T, bool>) = 
        Stream.filter (fun x -> func.Invoke(x)) stream

    static member Where<'T> (stream : ParStream<'T>, func :System.Func<'T, bool>) = 
        ParStream.filter (fun x -> func.Invoke(x)) stream

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

    


