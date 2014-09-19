
#time


#load "../../packages/MBrace.Runtime.0.5.4-alpha/bootstrap.fsx" 
#r "bin/Debug/Streams.Core.dll"
#r "bin/Debug/Streams.Cloud.dll"

open Nessos.Streams.Cloud
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

let rnd = new System.Random()
let data = [|1..100|] |> Array.map (fun i -> rnd.Next(100))

//let runtime = MBrace.InitLocal(totalNodes = 4, store = FileSystemStore.LocalTemp)
let run (cloud : Cloud<'T>) = 
    //runtime.Run <@ cloud @>
    MBrace.RunLocal cloud


data
|> CloudStream.ofArray 
|> CloudStream.sortBy id 10
|> CloudStream.toArray
|> run
    





