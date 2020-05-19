#### 0.6.0
* Remove net45 target.
* add sourcelink support.

#### 0.5.0
* Target netstandard2.0

#### 0.4.1
* New ParStream functions sortByDescending/sortByUsing

#### 0.4.0
* Fix Stream.sortBy issue.

#### 0.3.0
* New combinators (scan, head, tryHead)
* Fix the default Access level of SourceType

#### 0.2.9
* Fix of the annoying DegreeOfParallelism ref cell hack
* Fix ParStream.foldBy do not preserve order when requested to
* Fix combiner parameter to ParStream.foldBy is unused

#### 0.2.8
* Performance improvment for Stream.toSeq
* Support with withDegreeOfParallelism
* New cancellation mechanism for Stream/ParStream


#### 0.2.7
* New functions (ParStream/Stream.mapi, ParStream.toSeq)
* Array/ResizeArray specialization of (ParStream/Stream.ofSeq)
* Support for disposable iterators
* Correct order semantics

#### 0.2.6
* New Stream functions (empty, singleton, cast, concat, cache, groupUntil)
* Performance and memory improvements for ParStream functions (groupBy, foldBy)

#### 0.2.5
* Support for pull-based operators, implemented ofSeq and zip methods.

#### 0.2.4
* Introduce order semantics.

#### 0.2.3
* Added MaxBy,MinBy,AggregateBy/foldBy, Take, Skip and CountBy methods in Stream and ParStream.

#### 0.2.2
* ParStream bug fixes and performance improvements.

#### 0.2.0
* New functions (choose, tryFind, find, tryPick, pick, exists, forall) for Stream and ParStream.
* CSharp API

#### 0.1.5
* ParStream introduction
* Stream (sortBy, groupBy) API changes
* New Stream ResizeArray functions.

#### 0.1.0
* Initial Release.
