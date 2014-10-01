
#time


#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx" 
#r "bin/Debug/Streams.Core.dll"
#r "bin/Debug/Streams.Cloud.dll"

open Nessos.Streams.Cloud
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

let rnd = new System.Random()
let data = [|1..1000|] |> Array.map (fun i -> i)

let runtime = MBrace.InitLocal(totalNodes = 4, store = FileSystemStore.LocalTemp)
let run (cloud : Cloud<'T>) = 
    runtime.Run cloud 
    //MBrace.RunLocal cloud


//let cloudArray : = StoreClient.Default.CreateCloudArray("temp", data) 
let cloudArray = CloudArray.New("temp", [|1; 0|]) |> run


let cloudArray' = cloudArray.Cache()


let ca' =
    cloudArray'
    |> CloudStream.ofCloudArray 
    |> CloudStream.map (fun x -> x)
    |> CloudStream.toCloudArray
    |> run

[|1; 0|] = (ca' |> Seq.toArray)

