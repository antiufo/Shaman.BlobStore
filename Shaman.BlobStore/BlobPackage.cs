using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Shaman.Runtime
{
    internal class BlobPackage
    {
        internal PackageDirectory directory;
        internal string fileName;
        internal volatile MemoryStream ms;
        internal long lastCommittedLength;
        internal long startOfBlobHeader;
        internal long startOfBlobData;


        internal void Release(bool commit = true)
        {
            lock (BlobStore._lock)
            {
                BlobStore.Delete(Path.Combine(directory.path, this.currentFileName));
                if (commit) Commit(true);
                directory.busyPackages.Remove(this);

                if (ms.Length < (directory.packageSize ?? BlobStore.Configuration_PackageSize))
                {
                    directory.availablePackages.Add(this);
                }
                else
                {
                    WriteToFile(false);
                    this.ms = null;
                }
            }
        }

        internal BlobStream OpenWrite(bool autocommit)
        {
            return new BlobStream(this, autocommit);
        }

        public MemoryStream CopyMemoryStream()
        {
            return new MemoryStream(ms.GetBuffer(), 0, (int)ms.Length, false, true);

        }


        private string currentFileName;
        private DateTime? currentFileTime;

        internal MemoryStream EnsureAdditionalCapacity(int capacity)
        {
            return EnsureCapacity((int)ms.Length + capacity);
        }

        internal MemoryStream EnsureCapacity(int capacity)
        {
            if (capacity > ms.Capacity)
            {
                lock (BlobStore._lock)
                {
                    var arr = ms.GetBuffer();
                    var c = ms.Capacity;
                    while (c < capacity)
                    {
                        c *= 2;
                    }
                    var newarr = new byte[c];
                    var pos = (int)ms.Position;
                    var len = (int)ms.Length;
                    Buffer.BlockCopy(arr, 0, newarr, 0, len);
                    ms = new MemoryStream(newarr, 0, newarr.Length, true, true);
                    ms.Seek(pos, SeekOrigin.Begin);
                    ms.SetLength(len);
                }
            }
            return ms;
        }

        // must hold lock
        internal void WriteHeader(string name, DateTime? date)
        {
            if (ms == null)
            {
                var arr = new byte[directory.memoryStreamCapacity ?? BlobStore.Configuration_MemoryStreamCapacity];
                 ms = new MemoryStream(arr, 0, arr.Length, true, true);
            }
            lastFileCommitted = false;
            startOfBlobHeader = lastCommittedLength;
            ms.Seek(startOfBlobHeader, SeekOrigin.Begin);
            ms.SetLength(startOfBlobHeader);


            var nameBytes = Encoding.UTF8.GetBytes(name);
            EnsureAdditionalCapacity(nameBytes.Length + 60);
            ms.WriteByte(0);
            ms.WriteByte(0);
            ms.WriteByte(0);
            ms.WriteByte(0);
            currentFileName = name;
            currentFileTime = date;
            var nameLength = nameBytes.Length;
            if (date != null) nameLength += 8 + 1;
            ms.WriteByte((byte)(nameLength >> 0));
            ms.WriteByte((byte)(nameLength >> 8));
            ms.WriteByte(0);
            ms.WriteByte(0);
            ms.Write(nameBytes, 0, nameBytes.Length);
            if (date != null)
            {
                var dateBytes = BitConverter.GetBytes(date.Value.Ticks);
                ms.Write(dateBytes, 0, dateBytes.Length);
                ms.WriteByte(0);
            }
            startOfBlobData = ms.Length;
        }

        bool lastFileCommitted;

        // must hold lock
        internal long Commit(bool perfect)
        {
            if (lastFileCommitted) return ms.Length;

            ms.Seek(startOfBlobHeader, SeekOrigin.Begin);
            var length = (int)ms.Length;
            var blobLength = length - startOfBlobData;
            ms.WriteByte((byte)(blobLength >> 0));
            ms.WriteByte((byte)(blobLength >> 8));
            ms.WriteByte((byte)(blobLength >> 16));
            ms.WriteByte((byte)(blobLength >> 24));
            ms.Seek(2, SeekOrigin.Current);
            ms.WriteByte(perfect ? (byte)1 : (byte)0);
            ms.Seek(length, SeekOrigin.Begin);
            if (perfect)
            {
                lastCommittedLength = length;
                var location = new FileLocation()
                {
                    BlobName = currentFileName,
                    PackageFileName = this.fileName,
                    DataStartOffset = (int)startOfBlobData,
                    Package = this,
                    Length = length - (int)startOfBlobData,
                    Date = currentFileTime
                };
                directory.locations[currentFileName] = location;
                directory.locationsToAdd.Add(location);
            }
            lastFileCommitted = true;
            return length;
        }
        internal long bytesWrittenToDisk;



        // must hold lock
        internal void WriteToFile(bool close)
        {
            if (close)
            {
                if (!lastFileCommitted)
                {
                    ms.SetLength(startOfBlobHeader);
                    lastFileCommitted = true;
                }
            }

            var len = Commit(false);
            if (len != bytesWrittenToDisk)
            {
                using (var fs = File.Open(Path.Combine(directory.path, fileName), FileMode.OpenOrCreate, FileAccess.Write, FileShare.Delete))
                {
                    fs.Seek(bytesWrittenToDisk, SeekOrigin.Begin);
                    ms.Seek(bytesWrittenToDisk, SeekOrigin.Begin);
                    byte[] array = new byte[81920];
                    int count;
                    var remaining = len - bytesWrittenToDisk;
                    while (remaining != 0 && (count = ms.Read(array, 0, (int)Math.Min(array.Length, remaining))) != 0)
                    {
                        fs.Write(array, 0, count);
                    }
                }
            }
            bytesWrittenToDisk = len;
            if (close)
            {
                ms.Dispose();
            }
            else
            {
                ms.Seek(len, SeekOrigin.Begin);
            }
        }

    }
}
