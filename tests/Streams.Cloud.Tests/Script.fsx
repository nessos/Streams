
#time


#r "bin/Debug/MBrace.Core.dll"
#r "bin/Debug/MBrace.Lib.dll"
//#load "../../packages/MBrace.Runtime.0.5.4-alpha/bootstrap.fsx" 
#r "bin/Debug/Streams.Cloud.dll"

open Nessos.Streams.Cloud
open Nessos.MBrace

let data = [|1..10000000|]


data
|> CloudStream.ofArray 
|> CloudStream.filter (fun x -> x % 2 = 0)
|> CloudStream.length


