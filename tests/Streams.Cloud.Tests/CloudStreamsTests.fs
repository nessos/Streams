namespace Nessos.Streams.Tests
    open System.Linq
    open System.Collections.Generic
    open FsCheck
    open FsCheck.Fluent
    open NUnit.Framework
    open Nessos.Streams.Core
    open Nessos.Streams.Cloud
    open Nessos.MBrace
    open Nessos.MBrace.Client

    [<TestFixture>]
    type ``CloudStreams tests`` () =
        
        let run (cloud : Cloud<'T>) = MBrace.RunLocal cloud

        [<Test>]
        member __.``ofArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.length |> run
                let y = xs |> Seq.map ((+)1) |> Seq.length
                x = y).QuickCheckThrowOnFailure()

        
       
