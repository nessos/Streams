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



let ca' =
    cached
    |> CloudStream.ofCloudArray 
    |> CloudStream.map (fun x -> x * x)
    |> CloudStream.toCloudArray
    |> run

ca' |> Seq.toArray |> Seq.length


let path = @"C:\dev\github-repositories\MBrace.Demos\data\Shakespeare"

let cfs = runtime.GetStoreClient().UploadFiles(System.IO.Directory.GetFiles path)

open Nessos.Streams.Core

let r = 
    cfs
    |> CloudStream.ofCloudFiles CloudFile.ReadLines
    |> CloudStream.collect (fun lines -> Stream.ofSeq lines)
    |> CloudStream.map id
    |> CloudStream.length
    |> run

let cas = System.IO.Directory.GetFiles path
          |> Array.map (fun file -> let vs = System.IO.File.ReadLines(file) in runtime.GetStoreClient().CreateCloudArray("tmp", vs))
          |> Array.reduce (fun l r -> l.Append(r))

let r' = 
    cas
    |> CloudStream.ofCloudArray 
    |> CloudStream.map id
    |> CloudStream.length
    |> run


//cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.State }) |> Cloud.Parallel }
//|> run
//|> Seq.iter (
//    fun state ->
//        printfn "-------------------------------------" 
//        state |> Seq.sort |> (Seq.iter (printfn "%A")))
//
//CloudArrayCache.State 
//|> Seq.sort
//|> Seq.iter (printfn "%A")
//
//cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Guid }) |> Cloud.Parallel }
//|> run
//
//
//cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Occupied }) |> Cloud.Parallel }
//|> run
//
//let cloudArray = StoreClient.Default.CreateCloudArray<int>("temp", Seq.empty) 
//let cached = CloudStream.cache cloudArray |> run
//
//CloudArrayCache.Occupied