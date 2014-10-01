
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
    //runtime.Run cloud 
    MBrace.RunLocal cloud


//let cloudArray : = StoreClient.Default.CreateCloudArray("temp", data) 
let cloudArray = CloudArray.New("temp", data) |> run


cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.State }) |> Cloud.Parallel }
|> run
|> Seq.head
|> Seq.map (fun (_,s,e) -> s,e)
|> Seq.sortBy fst
|> Seq.toArray

cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Guid }) |> Cloud.Parallel }
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


[|1; 0|] = (ca' |> Seq.toArray)

