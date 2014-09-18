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
        let test = new ``CloudStreams tests`` ()
        //test.``ofArray`` ()

        let data = [|1..100|]

        let result = 
            data
            |> CloudStream.ofArray 
            |> CloudStream.countBy id
            |> MBrace.RunLocal
        
        0

