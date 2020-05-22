module Nessos.Streams.Benchmarks.Main

open BenchmarkDotNet.Running

[<EntryPoint>]
let main args =
    let assembly = System.Reflection.Assembly.GetExecutingAssembly()
    let switcher = new BenchmarkSwitcher(assembly)
    let summaries = switcher.Run(args)
    0