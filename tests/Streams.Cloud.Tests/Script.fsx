
#time


#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx" 
#r "bin/Debug/Streams.Core.dll"
#r "bin/Debug/Streams.Cloud.dll"

open Nessos.Streams.Cloud
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

let rnd = new System.Random()
let data = [|1..100|] |> Array.map (fun i -> i)

let runtime = MBrace.InitLocal(totalNodes = 4, store = FileSystemStore.LocalTemp)
let run (cloud : Cloud<'T>) = 
    runtime.Run cloud 
    //MBrace.RunLocal cloud


//let cloudArray = CloudArray.New("temp", data) |> run
let cloudArray = StoreClient.Default.CreateCloudArray("temp", data) 

cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.State }) |> Cloud.Parallel }
|> run
|> Seq.head
|> Seq.map (fun (_,t,s,e) -> t,s,e)
|> Seq.sortBy (fun (t,s,e) -> s,t)
|> Seq.toArray

cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Guid }) |> Cloud.Parallel }
|> run


cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Occupied }) |> Cloud.Parallel }
|> run

let cached = CloudStream.cache cloudArray
             |> run

let ca' =
    cached
    |> CloudStream.ofCloudArray 
    |> CloudStream.map (fun x -> x)
    |> CloudStream.toCloudArray
    |> run

ca' |> Seq.toArray |> Seq.length

run <| Cloud.GetTaskId()

[|1; 0|] = (ca' |> Seq.toArray)

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

getPartitions 3 0L 100L

Partitioner.Create(0L,100L).GetPartitions(3)
|> Seq.mapi (fun i x -> i, x |> toSeq )
