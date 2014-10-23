using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nessos.Streams.Cloud.CSharp.Tests
{
    [Serializable]
    internal class Custom1 { internal string Name; internal int Age; }

    [Serializable]
    internal class Custom2 { internal string Name { get; set; } internal int Age { get; set; } }

}
