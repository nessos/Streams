Streams
=======

A lightweight F# library for efficient functional-style pipelines on streams of data. 
The surface api follows the familiar F# pipelining
```fsharp
let data = [|1..10000000|]
let result = 
  data
  |> Stream.ofArray
  |> Stream.filter (fun x -> x % 2L = 0L)
  |> Stream.map (fun x -> x + 1L)
  |> Stream.sum
```
