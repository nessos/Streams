
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


let ca' =
    cloudArray
    |> CloudStream.ofCloudArray 
    |> CloudStream.map (fun x -> x * x)
    |> CloudStream.toCloudArray
    |> run

ca'.Container

//-------------------------------------------------------------------
// Wordcount
open System
open System.IO
open Nessos.Streams.Core

let path = Path.Combine(__SOURCE_DIRECTORY__,"../../../MBrace.Demos/data/Shakespeare")
let files = Directory.GetFiles(path)

let cas = files |> Array.map (fun f -> MBrace.RunLocal <@ CloudArray.New("shakespeare", File.ReadLines(f)) @>)
let text = cas |> Seq.reduce (fun l r -> l.Append(r))

/// words ignored by wordcount
let noiseWords = 
    set [
        "a"; "about"; "above"; "all"; "along"; "also"; "although"; "am"; "an"; "any"; "are"; "aren't"; "as"; "at";
        "be"; "because"; "been"; "but"; "by"; "can"; "cannot"; "could"; "couldn't"; "did"; "didn't"; "do"; "does"; 
        "doesn't"; "e.g."; "either"; "etc"; "etc."; "even"; "ever";"for"; "from"; "further"; "get"; "gets"; "got"; 
        "had"; "hardly"; "has"; "hasn't"; "having"; "he"; "hence"; "her"; "here"; "hereby"; "herein"; "hereof"; 
        "hereon"; "hereto"; "herewith"; "him"; "his"; "how"; "however"; "I"; "i.e."; "if"; "into"; "it"; "it's"; "its";
        "me"; "more"; "most"; "mr"; "my"; "near"; "nor"; "now"; "of"; "onto"; "other"; "our"; "out"; "over"; "really"; 
        "said"; "same"; "she"; "should"; "shouldn't"; "since"; "so"; "some"; "such"; "than"; "that"; "the"; "their"; 
        "them"; "then"; "there"; "thereby"; "therefore"; "therefrom"; "therein"; "thereof"; "thereon"; "thereto"; 
        "therewith"; "these"; "they"; "this"; "those"; "through"; "thus"; "to"; "too"; "under"; "until"; "unto"; "upon";
        "us"; "very"; "viz"; "was"; "wasn't"; "we"; "were"; "what"; "when"; "where"; "whereby"; "wherein"; "whether";
        "which"; "while"; "who"; "whom"; "whose"; "why"; "with"; "without"; "would"; "you"; "your" ; "have"; "thou"; "will"; 
        "shall"
    ]

[<Cloud>]
let getTop count =
    text
    |> CloudStream.ofCloudArray
    |> CloudStream.flatMap (fun line -> 
        line.Split([|' '; '.'; ','|], StringSplitOptions.RemoveEmptyEntries) 
        |> Stream.ofArray
        |> Stream.map (fun word -> word.ToLower())
        |> Stream.map (fun word -> word.Trim()))
    |> CloudStream.filter (fun word -> word.Length > 3 && not <| noiseWords.Contains(word))
    |> CloudStream.countBy id
    |> CloudStream.sortBy (fun (_,c) -> -c) count
    |> CloudStream.toCloudArray

let rt = MBrace.InitLocal(totalNodes = 4)
let result = rt.Run <@ getTop 20 @>

result |> Seq.iter (printfn "%A")


