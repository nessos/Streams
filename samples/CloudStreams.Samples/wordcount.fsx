
#load "../../packages/MBrace.Runtime.0.5.7-alpha/bootstrap.fsx" 
#r "../../bin/Streams.dll"
#r "../../bin/Streams.Cloud.dll"

open System
open System.IO
open System.Text.RegularExpressions
open Nessos.MBrace
open Nessos.MBrace.Client
open Nessos.Streams
open Nessos.Streams.Cloud

let path = "path to your files"
let files = Directory.GetFiles(path)

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

let splitWords =
    let regex = new Regex(@"[\W]+", RegexOptions.Compiled)
    fun word -> regex.Split(word)


let lines = 
    files
    |> Array.map (fun f -> StoreClient.Default.CreateCloudArrayAsync("tmp", File.ReadLines(f)))
    |> Async.Parallel
    |> Async.RunSynchronously
    |> Array.reduce (fun l r -> l.Append(r))

[<Cloud>]
let getTop count =
    lines
    |> CloudStream.ofCloudArray
    |> CloudStream.collect (fun line -> 
        splitWords line
        |> Stream.ofArray
        |> Stream.map (fun word -> word.ToLower())
        |> Stream.map (fun word -> word.Trim()))
    |> CloudStream.filter (fun word -> word.Length > 3 && not <| noiseWords.Contains(word))
    |> CloudStream.countBy id
    |> CloudStream.sortBy (fun (_,c) -> -c) count
    |> CloudStream.toCloudArray

let runtime = MBrace.InitLocal(totalNodes = 4)

let proc = runtime.CreateProcess <@ getTop 20 @>

proc.AwaitResult() |> Seq.iter (printfn "%A")
