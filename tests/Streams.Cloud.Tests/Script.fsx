
#time


#load "../../packages/MBrace.Runtime.0.5.4-alpha/bootstrap.fsx" 
#r "bin/Debug/Streams.Core.dll"
#r "bin/Debug/Streams.Cloud.dll"

open Nessos.Streams.Cloud
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

let data = [|1..10|]


data
|> CloudStream.ofArray 
|> CloudStream.filter (fun x -> x % 2 = 0)
|> CloudStream.length
|> MBrace.RunLocal

