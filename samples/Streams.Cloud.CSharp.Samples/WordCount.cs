using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams.CSharp;
using Nessos.Streams.Cloud.CSharp;
using Nessos.Streams.Cloud.CSharp.MBrace;
using System.Text.RegularExpressions;

namespace Nessos.Streams.Cloud.CSharp.Samples
{
    static class WordCount
    {
        static string[] files;

        #region NoiseWords
        static HashSet<string> noiseWords = 
            new HashSet<string> {
                "a", "about", "above", "all", "along", "also", "although", "am", "an", "any", "are", "aren't", "as", "at",
                "be", "because", "been", "but", "by", "can", "cannot", "could", "couldn't", "did", "didn't", "do", "does", 
                "doesn't", "e.g.", "either", "etc", "etc.", "even", "ever","for", "from", "further", "get", "gets", "got", 
                "had", "hardly", "has", "hasn't", "having", "he", "hence", "her", "here", "hereby", "herein", "hereof", 
                "hereon", "hereto", "herewith", "him", "his", "how", "however", "I", "i.e.", "if", "into", "it", "it's", "its",
                "me", "more", "most", "mr", "my", "near", "nor", "now", "of", "onto", "other", "our", "out", "over", "really", 
                "said", "same", "she", "should", "shouldn't", "since", "so", "some", "such", "than", "that", "the", "their", 
                "them", "then", "there", "thereby", "therefore", "therefrom", "therein", "thereof", "thereon", "thereto", 
                "therewith", "these", "they", "this", "those", "through", "thus", "to", "too", "under", "until", "unto", "upon",
                "us", "very", "viz", "was", "wasn't", "we", "were", "what", "when", "where", "whereby", "wherein", "whether",
                "which", "while", "who", "whom", "whose", "why", "with", "without", "would", "you", "your" , "have", "thou", "will", 
                "shall"
            }; 
        #endregion

        #region Helper functions
        static string[] SplitWords(this string text)
        {
            var regex = new Regex(@"[\W]+", RegexOptions.Compiled);
            return regex.Split(text);
        }

        static string WordTransform(this string word)
        {
            return word.Trim().ToLower();
        }

        static bool WordFilter(this string word)
        {
            return word.Length > 3 && !noiseWords.Contains(word);
        } 
        #endregion

        public static string FilesPath { set { files = Directory.GetFiles(value); } } 

        public static IEnumerable<Tuple<string,long>> RunWithCloudFiles(Runtime runtime)
        {
            var cfiles = StoreClient.Default.UploadFiles(files, "wordcount");
            var count = 20;

            var query = cfiles
                            .OfCloudFiles(CloudFile.ReadLines)
                            .SelectMany(lines => lines.AsStream())
                            .SelectMany(line => line.SplitWords().AsStream().Select(WordTransform))
                            .Where(WordFilter)
                            .CountBy(w => w)
                            .OrderBy(t => -t.Item2, count) 
                            .ToArray();

            var result = runtime.Run(query);
            return result;
        }

        public static IEnumerable<Tuple<string, long>> RunWithCloudArray(Runtime runtime)
        {
            var clines = files
                            .Select(file => StoreClient.Default.CreateCloudArray("tmp", File.ReadLines(file)))
                            .Aggregate((l, r) => l.Append(r));

            var count = 20;

            var query = clines
                            .AsCloudStream()
                            .SelectMany(line => line.SplitWords().AsStream().Select(WordTransform))
                            .Where(WordFilter)
                            .CountBy(w => w)
                            .OrderBy(t => -t.Item2, count)
                            .ToArray();

            var result = runtime.Run(query);
            return result;
        }
    }
}
