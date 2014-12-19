#time

#r "../../bin/Streams.Core.dll"
#r "../../bin/Streams.CSharp.dll"

open System
open System.Linq
open Nessos.Streams
open Nessos.Streams.CSharp


[|1|] |> Stream.ofArray |> Stream.groupUntil true (fun i -> i % 7 <> 0) |> Stream.toSeq |> Array.concat


let data = [|1..100000000|] |> Array.map (fun i -> int64 <| (i % 1000000))


data
|> Stream.ofArray
|> Stream.maxBy id

data
|> ParStream.ofArray
|> ParStream.maxBy id

data
|> ParStream.ofArray
|> ParStream.minBy id

data
|> Stream.ofArray
|> Stream.minBy id

data
|> Seq.filter (fun x -> x % 2L = 0L)
|> Seq.map (fun x -> x + 1L)
|> Seq.map (fun x -> x + 1L)
|> Seq.map (fun x -> x + 1L)
|> Seq.map (fun x -> x + 1L)
|> Seq.map (fun x -> x + 1L)
|> Seq.sum

data
|> Array.filter (fun x -> x % 2L = 0L)
|> Array.map (fun x -> x + 1L)
|> Array.map (fun x -> x + 1L)
|> Array.map (fun x -> x + 1L)
|> Array.map (fun x -> x + 1L)
|> Array.map (fun x -> x + 1L)
|> Array.sum

data
|> Stream.ofArray
|> Stream.filter (fun x -> x % 2L = 0L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.sum

data
|> ParStream.ofArray
|> ParStream.filter (fun x -> x % 2L = 0L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.sum


data.AsParallel()
    .Where(fun x -> x % 2L = 0L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Sum()


data.AsParStream()
    .Where(fun x -> x % 2L = 0L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Sum()

   
   


#r "../../packages/FSharp.Collections.ParallelSeq.1.0.2/lib/net40/FSharp.Collections.ParallelSeq.dll"
open FSharp.Collections.ParallelSeq

for i in 1..100 do
    System.Threading.Thread.Sleep 1000
    printfn "Testing..."
    let xs = [| for i in 1..10000 -> i |]

    let x = xs |> ParStream.ofArray |> ParStream.map ((+) 1) |> ParStream.sortBy id |> ParStream.toArray
    let y = xs |> Seq.map ((+) 1) |> Seq.sortBy id |> Seq.toArray
    assert(x = y)

data
|> PSeq.map (fun x -> x + 1L)
|> PSeq.groupBy id
|> PSeq.length

data
|> ParStream.ofArray
|> ParStream.map (fun x -> x + 1L)
|> ParStream.groupBy id
|> ParStream.length



let data = [|1..30000000|]

open System.Linq

data.AsParallel()
    .Select(fun x -> x + 1)
    .ToArray()

(Seq.initInfinite (fun i -> i))
    .AsParallel()
    .Select(fun x i -> (x, i))
    .Skip(10)
    .Take(10000000)
    .Select(fun x i -> (x, i))
    .ToArray()



|> Seq.toArray
|> ParStream.ofSeq
|> ParStream.mapi (fun i v -> (i, v))
|> ParStream.toArray



(Seq.initInfinite (fun i -> i))
|> Seq.mapi (fun i v -> (v, i))
|> Seq.skip 10
|> Seq.take 10000000
|> Seq.mapi (fun i v -> (v, i))
|> Seq.length


(Seq.initInfinite (fun i -> i))
|> Stream.ofSeq
|> Stream.mapi (fun i v -> (v, i))
|> Stream.skip 10
|> Stream.take 10000000
|> Stream.mapi (fun i v -> (v, i))
|> Stream.toArray

let data = [|1..100000000|]

data
|> ParStream.ofArray
|> ParStream.filter (fun x -> x % 2 = 0)
|> ParStream.map (fun x -> x + 1)
|> ParStream.length


data
|> Stream.ofArray
|> Stream.filter (fun x -> x % 2 = 0)
|> Stream.map (fun x -> x + 1)
|> Stream.length

data
|> Seq.filter (fun x -> x % 2 = 0)
|> Seq.map (fun x -> x + 1)
|> Seq.length


open System.Collections.Generic
open System.Collections.Concurrent

#time

let data = Enumerable.Range(1, 100000000)//.Select(fun x -> int64 x).ToArray()

data
|> ParStream.ofSeq
|> ParStream.filter (fun x -> true)
|> ParStream.mapi (fun i x -> x)
//|> ParStream.ordered
|> ParStream.length

data
|> Stream.ofSeq
|> Stream.filter (fun x -> true)
|> Stream.mapi(fun i x ->  i + x)
|> Stream.toSeq
|> Seq.length

data
|> Seq.filter (fun _ -> true)
|> Seq.mapi(fun i x ->  i + x)
|> Seq.length

let _items = [|0..1|]

for d in data do
    Array.Clear(_items, 0, 1)



data
|> ParStream.ofSeq
|> ParStream.filter (fun x -> x % 2L = 0L)
|> ParStream.map (fun x -> int64 1 + x)
|> ParStream.sum



data.AsParallel()
    .Where(fun _ -> true)
    .Select(fun x i -> i)
    .Count()