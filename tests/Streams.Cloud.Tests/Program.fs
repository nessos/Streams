namespace Nessos.Streams.Cloud.Tests


open System
open Nessos.Streams.Core
open Nessos.Streams.Cloud
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

module Program = 


    [<EntryPoint>]
    let main argv = 
        let data = [|1..10000000|]

        let result = 
            data
            |> CloudStream.ofArray 
            |> CloudStream.filter (fun x -> x % 2 = 0)
            |> CloudStream.length
            |> MBrace.RunLocal
        
        0

