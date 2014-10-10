(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin/"

#r "Nessos.Steams.Core.dll"

(**

# Streams

A lightweight F#/C# library for efficient functional-style pipelines on streams of data. 

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      Install via <a href="https://nuget.org/packages/Streams">NuGet</a>:
      <pre>PM> Install-Package Streams</pre>
      <pre>PM> Install-Package Streams.CSharp</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

The main design behind Streams is inspired by Java 8 Streams and is based on the observation that many functional pipelines follow the pattern:

    ```
    source/generator |> lazy |> lazy |> lazy |> eager/reduce
    ```

* Source/generator are functions that create Streams like Stream.ofArray/Stream.init.
* Lazy functions take in streams and return streams like Stream.map/Stream.filter, these operations are fused together for efficient iteration.
* Eager/reduce are functions like Stream.iter/Stream.sum that force the Stream to evaluate up to that point.

The main difference between LINQ/Seq and Streams is that LINQ is about composing external iterators (Enumerable/Enumerator) and  Streams is based on the continuation-passing-style composition of internal iterators.

For simple pipelines we have observed performance improvements of a factor of four and for more complex pipelines the performance gains are even greater.
*)

open Nessos.Streams.Core

let data = [|1..10000000|] |> Array.map int64

// Sequential

// Real: 00:00:00.044, CPU: 00:00:00.046, GC gen0: 0, gen1: 0, gen2: 0
data
|> Stream.ofArray
|> Stream.filter (fun x -> x % 2L = 0L)
|> Stream.map (fun x -> x + 1L)
|> Stream.sum

// Real: 00:00:00.264, CPU: 00:00:00.265, GC gen0: 0, gen1: 0, gen2: 0
data
|> Seq.filter (fun x -> x % 2L = 0L)
|> Seq.map (fun x -> x + 1L)
|> Seq.sum

// Real: 00:00:00.217, CPU: 00:00:00.202, GC gen0: 0, gen1: 0, gen2: 0
data
|> Array.filter (fun x -> x % 2L = 0L)
|> Array.map (fun x -> x + 1L)
|> Array.sum

// Parallel
#r "../../packages/FSharp.Collections.ParallelSeq.1.0/lib/net40/FSharp.Collections.ParallelSeq.dll"
open FSharp.Collections.ParallelSeq

// Real: 00:00:00.017, CPU: 00:00:00.078, GC gen0: 0, gen1: 0, gen2: 0
data
|> ParStream.ofArray
|> ParStream.filter (fun x -> x % 2L = 0L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.sum

// Real: 00:00:00.045, CPU: 00:00:00.187, GC gen0: 0, gen1: 0, gen2: 0
data
|> PSeq.filter (fun x -> x % 2L = 0L)
|> PSeq.map (fun x -> x + 1L)
|> PSeq.sum

(**
## References

* [Clash of the Lambdas](http://arxiv.org/abs/1406.6631)

## Contributing and copyright

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests.

The library is available under the Apache License. 
For more information see the [License file][license] in the GitHub repository. 

  [gh]: https://github.com/nessos/MBrace
  [issues]: https://github.com/nessos/Streams/issues
  [license]: https://github.com/nessos/Streams/blob/master/LICENSE.md
*)
