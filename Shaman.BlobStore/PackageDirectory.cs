using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Shaman.Runtime
{
    class PackageDirectory
    {
        internal List<BlobPackage> availablePackages = new List<BlobPackage>();
        internal List<BlobPackage> busyPackages = new List<BlobPackage>();
        internal string path;
        internal Dictionary<string, FileLocation> locations;
        internal List<FileLocation> locationsToAdd = new List<FileLocation>();
        internal int? packageSize;
        internal int? memoryStreamCapacity;



        public void Flush(bool close)
        {
            lock(BlobStore._lock)
            {
                foreach (var pkg in this.availablePackages)
                {
                    pkg.WriteToFile();
                    if (close) pkg.ms = null;
                }
                if (close) availablePackages.Clear();
                foreach (var pkg in this.busyPackages)
                {
                    pkg.WriteToFile();
                }
                if (this.locationsToAdd.Count != 0)
                {
                    using (var stream = File.Open(Path.Combine(this.path, ".shaman-blob-index"), FileMode.Append, FileAccess.Write, FileShare.Delete))
                    using (var bw = new BinaryWriter(stream, Encoding.UTF8))
                    {
                        string prevPackage = null;
                        foreach (var item in locationsToAdd)
                        {
                            if (prevPackage != item.PackageFileName)
                            {
                                prevPackage = item.PackageFileName;
                                bw.Write((byte)1);
                                bw.Write(item.PackageFileName ?? string.Empty);
                            }
                            else
                            {
                                bw.Write((byte)0);
                            }
                            bw.Write(item.BlobName);
                            bw.Write(item.DataStartOffset);
                        }
                    }
                    locationsToAdd.Clear();
                }
            }
        }
    }
}
