(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../src/Streams/bin/Release/netstandard2.0"
#r "Streams.dll"

open Nessos.Streams

(**

<style type="text/css">
    <!-- BenchmarkDotNet table style -->
	.bdn table { border-collapse: collapse; display: block; width: 100%; overflow: auto; }
	.bdn td, th { padding: 6px 13px; border: 1px solid #ddd; text-align: right; }
	.bdn tr { background-color: #fff; border-top: 1px solid #ccc; }
	.bdn tr:nth-child(even) { background: #f8f8f8; }
</style>

# Streams

A lightweight F#/C# library for efficient functional-style pipelines on streams of data. 

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      Install via <a href="https://nuget.org/packages/Streams">NuGet</a>:
      <pre>PM> Install-Package Streams
PM> Install-Package Streams.CSharp</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

The main design behind Streams is inspired by Java 8 Streams and is based on the observation that many functional pipelines follow the pattern:

    source/generator |> lazy |> lazy |> lazy |> eager/reduce

* Source/generator are functions that create Streams like Stream.ofArray/Stream.init.
* Lazy functions take in streams and return streams like Stream.map/Stream.filter, these operations are fused together for efficient iteration.
* Eager/reduce are functions like Stream.iter/Stream.sum that force the Stream to evaluate up to that point.

The main difference between LINQ/Seq and Streams is that LINQ is about composing external iterators (Enumerable/Enumerator) and  Streams is based on the continuation-passing-style composition of internal iterators.

## Example pipeline

*)

open Nessos.Streams

[|1L .. 10000000L|]
|> Stream.ofArray
|> Stream.filter (fun x -> x % 2L = 0L)
|> Stream.map (fun x -> x + 1L)
|> Stream.sortBy id
|> Stream.take 99
|> Stream.sum

(**

## Performance

The Streams library provides performance benefits compared to other LINQ-style combinator libraries.
Please see the benchmarks in [Performance.md](https://github.com/nessos/Streams/blob/master/docs/Performance.md) for more information.

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
