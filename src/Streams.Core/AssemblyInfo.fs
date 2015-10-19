namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Streams")>]
[<assembly: AssemblyProductAttribute("Streams")>]
[<assembly: AssemblyCompanyAttribute("Nessos Information Technologies")>]
[<assembly: AssemblyCopyrightAttribute("© Nessos Information Technologies.")>]
[<assembly: AssemblyTrademarkAttribute("MBrace")>]
[<assembly: AssemblyVersionAttribute("0.3.0")>]
[<assembly: AssemblyFileVersionAttribute("0.3.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.3.0"
