namespace Nessos.Streams.Benchmarks

open System.Linq
open Nessos.Streams
open BenchmarkDotNet.Attributes

[<MemoryDiagnoser>]
type SimpleBenchmark() =

    let data = [|1L..10000000L|] |> Array.map int64

    [<Benchmark(Description = "LINQ Pipeline", Baseline = true)>]
    member _.LinqPipeline() =
        data.Where(fun x -> x % 2L = 0L)
            .Select(fun x -> x + 1L)
            .Sum()
        |> ignore

    [<Benchmark(Description = "F# Seq Pipeline")>]
    member _.SeqPipeline() =
        data
        |> Seq.filter (fun x -> x % 2L = 0L)
        |> Seq.map (fun x -> x + 1L)
        |> Seq.sum
        |> ignore
    
    [<Benchmark(Description = "Nessos Stream Pipeline")>]
    member _.StreamPipeline() =
        data
        |> Stream.ofArray
        |> Stream.filter (fun x -> x % 2L = 0L)
        |> Stream.map (fun x -> x + 1L)
        |> Stream.sum
        |> ignore
