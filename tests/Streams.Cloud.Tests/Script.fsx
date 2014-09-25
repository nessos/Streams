
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

//let runtime = MBrace.InitLocal(totalNodes = 4, store = FileSystemStore.LocalTemp)
let run (cloud : Cloud<'T>) = 
    //runtime.Run cloud 
    MBrace.RunLocal cloud


let cloudArray = CloudArray.New("temp", data) |> run



cloudArray
|> CloudStream.ofCloudArray 
|> CloudStream.length
|> run
    


let xs = [|1;0;0;0;0;0;0;0;0;0;0;0|]
let x = xs |> CloudStream.ofArray |> CloudStream.sortBy id 12 |> CloudStream.toArray |> run
let y = (xs |> Seq.sortBy id) |> Seq.take 12 |> Seq.toArray

