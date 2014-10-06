#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx" 
#r "bin/Debug/Streams.Core.dll"
#r "bin/Debug/Streams.Cloud.dll"

open Nessos.Streams.Cloud
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

#time

let rnd = new System.Random()
let data = Array.init 100 id

let runtime = MBrace.InitLocal(totalNodes = 4, store = FileSystemStore.LocalTemp)
let run (cloud : Cloud<'T>) = 
    runtime.Run cloud 
    //MBrace.RunLocal cloud


cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.State }) |> Cloud.Parallel }
|> run
|> Seq.iter (
    fun state ->
        printfn "-------------------------------------" 
        state |> Seq.sort |> (Seq.iter (printfn "%A")))

CloudArrayCache.State 
|> Seq.sort
|> Seq.iter (printfn "%A")

cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Guid }) |> Cloud.Parallel }
|> run


cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Occupied }) |> Cloud.Parallel }
|> run

let cloudArray = StoreClient.Default.CreateCloudArray<int>("temp", Seq.empty) 
let cached = CloudStream.cache cloudArray |> run

CloudArrayCache.Occupied

let ca' =
    cached
    |> CloudStream.ofCloudArray 
    |> CloudStream.map (fun x -> x * x)
    |> CloudStream.toCloudArray
    |> run

ca' |> Seq.toArray |> Seq.length



open System.Collections.Generic
open System.Collections.Concurrent

let toSeq (enum : IEnumerator<_>)= 
    seq {
        while enum.MoveNext() do
            yield enum.Current
    }
let getPartitions (totalWorkers : int) (s : int64) (e : int64) = 
    let partitioner = Partitioner.Create(s, e)
    let partitions = partitioner.GetPartitions(totalWorkers) 
                        |> Seq.collect toSeq 
                        |> Seq.toArray 
    partitions


getPartitions 8 0L 100L

Partitioner.Create(0L,100L).GetPartitions(3)
|> Seq.mapi (fun i x -> i, x |> toSeq )

let xs = [1..2000]

let p = Partitioner.Create(xs)

let ps = p.GetPartitions(4)
         |> Seq.map (toSeq >> Seq.toArray)
         |> Seq.toArray 





