using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Shaman.Runtime
{
    internal class FileLocation
    {
        public int DataStartOffset;
        public string PackageFileName;
        public string BlobName;
        internal BlobPackage Package;
        internal int? Length;
        public DateTime? Date;
    }
}
