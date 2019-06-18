// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/build/FAKE/tools"
#r "packages/build/FAKE/tools/FakeLib.dll"

open Fake
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper

open System
open System.IO

let project = "Streams"
let artifactsDir = __SOURCE_DIRECTORY__ @@ "artifacts"

// --------------------------------------------------------------------------------------
// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md") 

let gitHome = "https://github.com/nessos"
let gitName = "Streams"

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let attributes =
        [ 
            Attribute.Title project
            Attribute.Product project
            Attribute.Company "Nessos Information Technologies"
            Attribute.Copyright "\169 Nessos Information Technologies."
            Attribute.Trademark "MBrace"
            Attribute.Version release.AssemblyVersion
            Attribute.FileVersion release.AssemblyVersion
        ]

    !! "./src/**/AssemblyInfo.fs"
    |> Seq.iter (fun info -> CreateFSharpAssemblyInfo info attributes)
    !! "./src/**/AssemblyInfo.cs"
    |> Seq.iter (fun info -> CreateCSharpAssemblyInfo info attributes)
)


// --------------------------------------------------------------------------------------
// Clean and restore packages

Target "Clean" (fun _ ->
    CleanDirs (!! "**/bin/Release/")
    CleanDir artifactsDir
)

//
//// --------------------------------------------------------------------------------------
//// Build library & test project

let configuration = environVarOrDefault "Configuration" "Release"

Target "Build" (fun _ ->
    DotNetCli.Build(fun config ->
        { config with
            Project = project + ".sln"
            Configuration = configuration }
    )
)


// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

let testProjects = 
    [
        yield "tests/Streams.Tests/"
        yield "tests/Streams.Tests.CSharp/"
    ]

Target "RunTests" (fun _ ->
    for proj in testProjects do
        DotNetCli.Test (fun c ->
            { c with
                Project = proj
                Configuration = configuration 
                AdditionalArgs = ["--no-build"] })
)

//// --------------------------------------------------------------------------------------
//// Build a NuGet package

Target "NuGet" (fun _ ->    
    Paket.Pack (fun p -> 
        { p with 
            ToolPath = ".paket/paket.exe" 
            OutputPath = artifactsDir
            Version = release.NugetVersion
            ReleaseNotes = toLines release.Notes })
)

Target "NuGetPush" (fun _ -> Paket.Push (fun p -> { p with WorkingDir = artifactsDir }))

//// --------------------------------------------------------------------------------------
//// Document generation

Target "GenerateDocs" (fun _ ->
    executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"] [] |> ignore
)

Target "ReleaseDocs" (fun _ ->
    let tempDocsDir = "temp/gh-pages"
    CleanDir tempDocsDir
    Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir

    fullclean tempDocsDir
    CopyRecursive "docs/output" tempDocsDir true |> tracefn "%A"
    StageAll tempDocsDir
    Commit tempDocsDir (sprintf "Update generated documentation for Streams %s" release.NugetVersion)
    Branches.push tempDocsDir
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Release" DoNothing

Target "Prepare" DoNothing
Target "PrepareRelease" DoNothing
Target "Default" DoNothing
Target "Help" (fun _ -> PrintTargets())
Target "Bundle" DoNothing

"Clean"
  ==> "AssemblyInfo"
  ==> "Prepare"
  ==> "Build"
  ==> "RunTests"
  ==> "Default"

"Build"
  ==> "PrepareRelease" 
  ==> "NuGet"
  //==> "GenerateDocs"
  ==> "Bundle"

"Bundle"
  //==> "ReleaseDocs"
  ==> "NuGetPush"
  ==> "Release"

// start build
RunTargetOrDefault "Default"