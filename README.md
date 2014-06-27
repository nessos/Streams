Streams
=======

A lightweight F# library for efficient functional-style pipelines on streams of data. The main design behind Streams
is inspired by Java 8 Streams and is based on the observation that functional pipelines follow the pattern
```fsharp 
source/generator |> lazy |> lazy |> lazy |> eager/reduce
```
* Source/generator are functions that create Streams like Stream.ofArray/Stream.init.
* Lazy functions take in streams and return streams like Stream.map/Stream.filter, these operatinos are fused together for efficient iteration.
* Eager/reduce are functions like Stream.iter/Stream.sum that force the Stream to evaluate up to that point.

The surface api follows the familiar F# pipelining
```fsharp
let data = [|1..10000000|]
let result = 
  data
  |> Stream.ofArray
  |> Stream.filter (fun x -> x % 2 = 0)
  |> Stream.map (fun x -> x * x)
  |> Stream.sum
```
