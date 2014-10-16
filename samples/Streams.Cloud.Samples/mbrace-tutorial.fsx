// A short introduction to using MBrace through F# Interactive
//
// For more about MBrace, see:
//    http://m-brace.net
//    http://nessos.github.io/MBrace
//

#load "../packages/MBrace.Runtime.0.5.8-alpha/bootstrap.fsx" 

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

[<Cloud>]
let sqr x = cloud { return x * x }

[<Cloud>]
let sumOfSquares n = cloud {
    let! results = [1 .. n] |> List.map sqr |> Cloud.Parallel
    return Array.sum results
}

//
//  Execute a cloud workflow in-memory with thread parallelism
//

MBrace.RunLocal <@ sumOfSquares 100 @>

//
//  Initialize an MBrace runtime with nodes running in the local machine.
//  This will spawn four nodes and organize them in a local MBrace cluster
//  The runtime will be using the system's temp folder as its store provider.
//

let runtime = MBrace.InitLocal(totalNodes = 4, store = FileSystemStore.LocalTemp)

runtime.ShowInfo() // print info on the cluster

let proc = runtime.CreateProcess <@ sumOfSquares 100 @> // initialize a computation on the cluster

runtime.ShowProcessInfo() // print cluster process info

proc.AwaitResult() // wait for the result

let node = Node.Spawn() // spawn an additional local node

runtime.Attach node // attach new node to cluster

runtime.Reboot() // reset the cluster and start anew

//
//  To initialize a runtime of remote nodes, the following steps need to be taken:
//

let node1 = Node.Connect "mbrace://hostname1:2675"
let node2 = Node.Connect "mbrace://hostname2:2675"
let node3 = Node.Connect "mbrace://hostname3:2675"

let runtime' = MBrace.Boot [ node1 ; node2 ; node3 ]
