namespace Nessos.Streams.Cloud.Tests


open System
open Nessos.Streams.Core
open Nessos.Streams.Cloud
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

module Program = 
    let data = [|1..100|]
    let comp = 
            data
            |> CloudStream.ofArray 
            |> CloudStream.countBy id
            |> CloudStream.toArray

    

    // set local MBrace executable location
    MBraceSettings.MBracedExecutablePath <- "../../../../packages/MBrace.Runtime.0.5.6-alpha/tools/mbraced.exe"

    //let runtime = MBrace.InitLocal(totalNodes = 4, store = FileSystemStore.LocalTemp)
    [<EntryPoint>]
    let main argv = 
        let cloudArray = CloudArray.New("temp", [|1..10|]) |> MBrace.RunLocal 
        let result = cloudArray |> CloudStream.ofCloudArray |> CloudStream.length |> MBrace.RunLocal 

        0

