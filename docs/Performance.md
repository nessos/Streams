# Performance

The Streams library provides performance benefits compared to other LINQ-style combinator libraries.
In this article we provide a collection of simple benchmarks comparing Streams to LINQ and F# seq, 
as well as their parallelized variants. Here's the hardware that we used for tests:

``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.264 (2004/?/20H1)
Intel Core i7-8665U CPU 1.90GHz (Coffee Lake), 1 CPU, 8 logical and 4 physical cores
.NET Core SDK=3.1.300
  [Host]     : .NET Core 3.1.4 (CoreCLR 4.700.20.20201, CoreFX 4.700.20.22101), X64 RyuJIT DEBUG
  DefaultJob : .NET Core 3.1.4 (CoreCLR 4.700.20.20201, CoreFX 4.700.20.22101), X64 RyuJIT
  
```

You can run the benchmarks yourself using the [Streams.Benchmarks](https://github.com/nessos/Streams/tree/master/tests/Streams.Benchmarks) project.

The benchmarks below measure variants of the following simple scenario:
```fsharp
[|1L .. 10000000L|]
|> Stream.ofArray
|> Stream.filter (fun x -> x % 2L = 0L)
|> Stream.map (fun x -> x + 1L)
|> Stream.sum
```

## Sequential

|                   Method |      Mean |    Error |    StdDev |    Median | Ratio | RatioSD | Gen 0 | Gen 1 | Gen 2 | Allocated |
|------------------------- |----------:|---------:|----------:|----------:|------:|--------:|------:|------:|------:|----------:|
|          &#39;LINQ Pipeline&#39; |  63.97 ms | 1.274 ms |  3.197 ms |  64.07 ms |  1.00 |    0.00 |     - |     - |     - |     280 B |
|        &#39;F# Seq Pipeline&#39; | 150.46 ms | 3.413 ms |  9.792 ms | 147.02 ms |  2.39 |    0.21 |     - |     - |     - |     352 B |
| &#39;Nessos Stream Pipeline&#39; |  41.00 ms | 3.494 ms | 10.302 ms |  42.35 ms |  0.71 |    0.12 |     - |     - |     - |     496 B |

## Parallel

|                      Method |     Mean |    Error |   StdDev | Ratio | RatioSD | Gen 0 | Gen 1 | Gen 2 | Allocated |
|---------------------------- |---------:|---------:|---------:|------:|--------:|------:|------:|------:|----------:|
|            &#39;PLINQ Pipeline&#39; | 24.63 ms | 0.485 ms | 0.711 ms |  1.00 |    0.00 |     - |     - |     - |   7.17 KB |
|          &#39;F# PSeq Pipeline&#39; | 30.59 ms | 0.585 ms | 0.650 ms |  1.25 |    0.04 |     - |     - |     - |   7.22 KB |
| &#39;Nessos ParStream Pipeline&#39; | 14.76 ms | 0.292 ms | 0.678 ms |  0.61 |    0.03 |     - |     - |     - |   7.09 KB |