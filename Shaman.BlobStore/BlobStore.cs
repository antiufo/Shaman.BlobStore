using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Shaman.Runtime
{
    public static class BlobStore
    {
        public static void WriteAllBytes(string path, byte[] data)
        {
            using (var stream = OpenWriteNoAutoCommit(path))
            {
                stream.Write(data, 0, data.Length);
                stream.Commit();
            }
            Debug.Assert(Exists(path));
        }

        private static void IndexDirectory(PackageDirectory dir, string directory)
        {
            dir.locations = new Dictionary<string, FileLocation>();
            var indexFile = Path.Combine(directory, ".shaman-blob-index");
            using (var stream = File.Open(indexFile + "$", FileMode.Create, FileAccess.Write, FileShare.Delete))
            using (var bw = new BinaryWriter(stream, Encoding.UTF8))
            {
                string prevPackage = null;
                foreach (var item in EnumerateFiles(directory, includeDeleted: false, orderByDate: false, singlePackage: null, allowIndex: false))
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


                    bw.Write((byte)0);

                    var length = (uint)item.Length.Value;
                    if (item.LastWriteTime != null) length |= (uint)1 << 31;

                    bw.Write(length);

                    if (item.LastWriteTime != null)
                    {
                        bw.Write(item.LastWriteTime.Value.Ticks);
                    }

                    bw.Write(item.Name);
                    bw.Write(item.DataStartOffset);

                    dir.locations[item.Name] = new FileLocation()
                    {
                        BlobName = item.Name,
                        DataStartOffset = item.DataStartOffset,
                        PackageFileName = item.PackageFileName,
                        Date = item.LastWriteTime,
                        Length = item.Length.Value
                    };
                }
            }
            File.Delete(indexFile);
            File.Move(indexFile + "$", indexFile);

        }


        internal static void Write7BitEncodedInt(BinaryWriter bw, int value)
        {
            // Write out an int 7 bits at a time.  The high bit of the byte,
            // when on, tells reader to continue reading more bytes.
            uint v = (uint)value;   // support negative numbers
            while (v >= 0x80)
            {
                bw.Write((byte)(v | 0x80));
                v >>= 7;
            }
            bw.Write((byte)v);
        }


        internal static int Read7BitEncodedInt(BinaryReader br)
        {
            // Read out an Int32 7 bits at a time.  The high bit
            // of the byte when on means to continue reading more bytes.
            int count = 0;
            int shift = 0;
            byte b;
            do
            {
                // Check for a corrupted stream.  Read a max of 5 bytes.
                // In a future version, add a DataFormatException.
                if (shift == 5 * 7)  // 5 bytes max per Int32, shift += 7
                    throw new FormatException("Format_Bad7BitInt32");

                // ReadByte handles end of stream cases for us.
                b = br.ReadByte();
                count |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }

        public static void WriteAllText(string path, string text, Encoding encoding)
        {
            using (var stream = OpenWriteNoAutoCommit(path))
            using (var streamWriter = new StreamWriter(stream, encoding))
            {
                streamWriter.Write(text);
                streamWriter.Flush();
                stream.Commit();
            }
            Debug.Assert(Exists(path));
        }


        public static void SetConfigurationForDirectory(string directory, int packageSizeApproxKB)
        {
            if (directory == null) throw new ArgumentNullException();
            directory = Path.GetFullPath(directory);
            var d = GetDirectory(directory);

            d.packageSize = (int)(0.8 * packageSizeApproxKB);
            d.memoryStreamCapacity = (int)(1 * packageSizeApproxKB);
        }

        public static BlobStream OpenWrite(string path)
        {
            var store = AcquireAndWriteHeader(path);
            return store.OpenWrite(true);
        }
        public static BlobStream OpenWriteNoAutoCommit(string path)
        {
            var store = AcquireAndWriteHeader(path);
            return store.OpenWrite(false);
        }

        public static BlobStream OpenWriteNoAutoCommit(string path, DateTime date)
        {
            var store = AcquireAndWriteHeader(path, date);
            return store.OpenWrite(false);
        }

        public static IEnumerable<string> GetOpenDirectories()
        {
            lock (_lock)
            {
                return directories.Keys.ToList();
            }
        }

        public static void CloseDirectory(string path)
        {
            if (path == null) throw new ArgumentNullException();
            lock (_lock)
            {
                path = Path.GetFullPath(path);

                Debug.Assert(path[0] == '/' || path[1] == ':');
                path = path.TrimEnd(trimChars);
                PackageDirectory dir;
                directories.TryGetValue(path, out dir);
                if (dir != null)
                {
                    dir.Flush(true);
                    directories.Remove(path);
                    foreach (var b in dir.busyPackages.Union(dir.availablePackages))
                    {
                        b.directory = null;
                        b.fileName = null;
                        b.ms?.Dispose();
                        b.ms = null;
                    }
                    dir.availablePackages = null;
                    dir.busyPackages = null;
                    dir.locations = null;
                    dir.locationsToAdd = null;
                }
            }
        }

        public class Blob
        {
            public int DataStartOffset { get; internal set; }

            public string Name { get; internal set; }

            public string Path { get; internal set; }
            public int? Length { get; internal set; }
            public bool? ClosedCleanly { get; internal set; }
            public string PackagePath { get; internal set; }
            public string PackageFileName { get; internal set; }
            public bool Deleted { get; internal set; }

            public DateTime? LastWriteTime { get; internal set; }

            internal Stream fileStream;

            public Stream OpenRead()
            {
                if (fileStream == null)
                {
                    return BlobStore.OpenRead(this.Path);
                }

                int len;
                if (Length != null) len = Length.Value;
                else
                {
                    throw new Exception("Length should've been available, since fileStream is not null.");
                }

                fileStream.Seek(DataStartOffset, SeekOrigin.Begin);
                
                var f = new BlobStream(fileStream, len, true);
                fileStream = null;
                return f;
            }

            public string ReadAllText()
            {
                using (var sr = new StreamReader(OpenRead(), Encoding.UTF8, true))
                {
                    return sr.ReadToEnd();
                }
            }
            public byte[] ReadAllBytes()
            {
                using (var stream = OpenRead())
                {
                    var arr = new byte[stream.Length];
                    int offset = 0;
                    int readBytes;
                    while ((readBytes = stream.Read(arr, offset, arr.Length - offset)) != 0)
                    {
                        offset += readBytes;
                    }
                    return arr;
                }
            }
        }

        public static IEnumerable<Blob> EnumerateFiles(Stream package)
        {
            if (!package.CanSeek) package = new UnseekableStreamWrapper(package, package.Length);
            return EnumerateFiles(package, includeDeleted: false);
        }
        public static IEnumerable<Blob> EnumerateFiles(Stream package, bool includeDeleted)
        {
            if (!package.CanSeek) package = new UnseekableStreamWrapper(package, package.Length);
            return EnumerateFiles(null, includeDeleted: includeDeleted, orderByDate: false, singlePackage: package, allowIndex: false);
        }
        public static IEnumerable<Blob> EnumerateFiles(Stream package, bool includeDeleted, long packageLength)
        {
            if (!package.CanSeek) package = new UnseekableStreamWrapper(package, packageLength);
            return EnumerateFiles(null, includeDeleted: includeDeleted, orderByDate: false, singlePackage: package, allowIndex: false);
        }

        public static IEnumerable<Blob> EnumerateFiles(string directory)
        {
            return EnumerateFiles(directory, includeDeleted: false, orderByDate: false, singlePackage: null, allowIndex: true);
        }
        public static IEnumerable<Blob> EnumerateFiles(string directory, string pattern)
        {
            return EnumerateFiles(directory, pattern: pattern, includeDeleted: false, orderByDate: false, singlePackage: null, allowIndex: true);
        }

        public static IEnumerable<Blob> EnumerateFiles(string directory, bool includeDeleted)
        {
            return EnumerateFiles(directory, includeDeleted: true, orderByDate: false, singlePackage: null, allowIndex: true);
        }

        public static IEnumerable<Blob> EnumerateFiles(string directory, string pattern, bool includeDeleted, bool orderByDate, Stream singlePackage, bool allowIndex)
        {
            if (pattern == "*") pattern = null;
            string prefix = null;
            string suffix = null;
            if (pattern != null)
            {
                var star = pattern.IndexOf('*');
                if (star == -1 || pattern.LastIndexOf('*') != star) throw new ArgumentException("Exactly one wildcard (*) character must be specified.");
                prefix = pattern.Substring(0, star);
                suffix = pattern.Substring(star + 1);
            }
            var items = EnumerateFiles(directory, includeDeleted: includeDeleted, orderByDate: orderByDate, singlePackage: null, allowIndex: allowIndex);
            if (pattern != null) items = items.Where(x => x.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) && x.Name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase));
            return items;
        }


        public static IEnumerable<Blob> EnumerateFiles(string directory, bool includeDeleted, bool orderByDate, Stream singlePackage, bool allowIndex)
        {
            if ((directory == null) == (singlePackage == null)) throw new ArgumentException();

            if (allowIndex && singlePackage == null)
            {
                lock (_lock)
                {
                    var dir = GetDirectory(directory);
                    var pkgs = dir.locations.Values.GroupBy(x => x.PackageFileName);
                    if (orderByDate) pkgs = pkgs.Select(x =>
                    {
                        var z = Path.Combine(directory, x.Key);
                        return new
                        {
                            Date = File.Exists(z) ? File.GetCreationTimeUtc(z) : DateTime.MaxValue,
                            Package = x
                        };
                    }).OrderBy(x => x.Date).Select(x => x.Package);
                    string currentPackageName = null;
                    string currentPackagePath = null;
                    var k = pkgs.SelectMany(x => x.OrderBy(y => y.DataStartOffset)).Select(x => 
                    {
                        if (x.PackageFileName != currentPackageName)
                        {
                            currentPackagePath = Path.Combine(directory, x.PackageFileName);
                            currentPackageName = x.PackageFileName;
                        }
                        return new Blob()
                        {
                            LastWriteTime = x.Date,
                            DataStartOffset = x.DataStartOffset,
                            Deleted = x.DataStartOffset == -1,
                            Name = x.BlobName,
                            Length = x.Length,
                            PackageFileName = x.PackageFileName,
                            PackagePath = currentPackagePath,
                            Path = Path.Combine(directory, x.BlobName),
                            ClosedCleanly = null,
                        };
                    });
                    if (!includeDeleted) k = k.Where(x => !x.Deleted);
                    return k.ToList();
                }

            }
            return EnumerateFilesNoIndex(directory, includeDeleted, orderByDate, singlePackage);

        }

        private static IEnumerable<Blob> EnumerateFilesNoIndex(string directory, bool includeDeleted, bool orderByDate, Stream singlePackage)
        {
            


            IEnumerable<string> packages;
            if (directory != null)
            {
                directory = Path.GetFullPath(directory);

                packages =
#if NET35
            Directory.GetFiles(
#else
            Directory.EnumerateFiles(
#endif
                directory, "*.shaman-blobs");

                if (orderByDate)
                {
                    packages = packages.Select(x => new { Path = x, Date = File.GetCreationTimeUtc(x) }).OrderBy(x => x.Date).Select(x => x.Path);
                }

            }
            else
            {
                packages = new string[1];
            }
            foreach (var pkg in packages)
            {
                using (var fs = singlePackage ?? File.Open(pkg, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.ReadWrite))
                {
                    Blob previous = null;
                    var packagePath = pkg;
                    while (true)
                    {
                        var first = fs.ReadByte();
                        if (first == -1) break;
                        var len = 0;
                        len |= first << 0;
                        len |= ReadByteChecked(fs) << 8;
                        len |= ReadByteChecked(fs) << 16;
                        len |= ReadByteChecked(fs) << 24;


                        var nl = 0;
                        nl |= ReadByteChecked(fs) << 0;
                        var d = ReadByteChecked(fs);
                        // var wasNameCorrupted = false;
                        // if (!SupportCorruptedFileNameLengths)
                        {
                            nl |= d << 8;
                        }
                        /* else
                         {
                             if (d != 0)
                             {
                                 wasNameCorrupted = true;
                                 Console.WriteLine("Fixing corrupted entry...");
                                 fs.Seek(-1, SeekOrigin.Current);
                                 fs.WriteByte(0);
                             }
                         }*/

                        var closedCleanly = ReadByteChecked(fs) == 1;
                        var deleted = ReadByteChecked(fs) == 1;
                        var nameBytes = new byte[nl];
                        while (nl != 0)
                        {
                            nl -= fs.Read(nameBytes, nameBytes.Length - nl, nl);
                        }
                        DateTime? date = null;
                        nl = nameBytes.Length;
                        if (nameBytes[nl - 1] == 0)
                        {
                            date = new DateTime(BitConverter.ToInt64(nameBytes, nl - 9), DateTimeKind.Utc);
                            nl -= 9;
                        }
                        var name = Encoding.UTF8.GetString(nameBytes, 0, nl);
                        var next = fs.Position + len;
                        var blob = new Blob()
                        {
                            PackageFileName = directory != null ? Path.GetFileName(pkg) : null,
                            PackagePath = packagePath,
                            Length = len,
                            Name = name,
                            Path = directory != null ? Path.Combine(directory, name) : null,
                            ClosedCleanly = closedCleanly,
                            DataStartOffset = (int)fs.Position,
                            fileStream = fs,
                            Deleted = deleted,
                            LastWriteTime = date
                        };
                        if (previous != null) previous.fileStream = null;
                        previous = blob;
                        if (!deleted || includeDeleted)
                            yield return blob;
                        fs.Seek(next, SeekOrigin.Begin);
                    }
                    if (previous != null) previous.fileStream = null;
                }
            }
            if (directory != null)
            {
                var additional = new List<Blob>();
                lock (_lock)
                {
                    var dir = GetDirectory(directory);

                    foreach (var file in dir.locationsToAdd)
                    {
                        FileLocation mostRecent;
                        dir.locations.TryGetValue(file.BlobName, out mostRecent);
                        if (mostRecent == file)
                        {
                            additional.Add(new Blob()
                            {
                                DataStartOffset = file.DataStartOffset,
                                PackageFileName = file.PackageFileName,
                                Name = file.BlobName,
                                ClosedCleanly = true,
                                Length = file.Length,
                                Path = Path.Combine(directory, file.BlobName)
                            });
                        }
                    }
                }
                foreach (var item in additional)
                {
                    yield return item;
                }
            }
        }

        // public static bool SupportCorruptedFileNameLengths;

        public static Stream OpenRead(string path)
        {
            if (path == null) throw new ArgumentNullException();
            path = Path.GetFullPath(path);
            var dirpath = Path.GetDirectoryName(path);
            var name = Path.GetFileName(path);
            lock (_lock)
            {
                var dir = GetDirectory(dirpath);
                FileLocation loc;
                if (!dir.locations.TryGetValue(name, out loc)) throw new FileNotFoundException("BlobStore file not found. ", path);
                if (loc.Package != null && loc.Package.ms != null)
                {
                    var ms2 = loc.Package.CopyMemoryStream();
                    ms2.Seek(loc.DataStartOffset, SeekOrigin.Begin);
                    return new BlobStream(ms2, loc.Length.Value, true);
                }
                else
                {
                    var fs = File.Open(Path.Combine(dirpath, loc.PackageFileName), FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.ReadWrite);
                    SeekToBeginOfBlobName(loc, fs);
                    fs.Seek(-8, SeekOrigin.Current);
                    var len = 0;
                    len |= fs.ReadByte() << 0;
                    len |= fs.ReadByte() << 8;
                    len |= fs.ReadByte() << 16;
                    len |= fs.ReadByte() << 24;
                    fs.Seek(loc.DataStartOffset, SeekOrigin.Begin);
                    return new BlobStream(fs, len, false);
                }

            }
        }

        public static bool Exists(string path)
        {
            if (string.IsNullOrEmpty(path)) return false;
            path = Path.GetFullPath(path);
            var blobName = Path.GetFileName(path);
            var dirpath = Path.GetDirectoryName(path);
            lock (_lock)
            {
                var dir = GetDirectory(dirpath);
                return dir.locations.ContainsKey(blobName);
            }
        }

        private static int ReadByteChecked(Stream fs)
        {
            var k = fs.ReadByte();
            if (k == -1) throw new EndOfStreamException();
            return k;
        }

        private static Dictionary<string, PackageDirectory> directories = new Dictionary<string, PackageDirectory>(StringComparer.OrdinalIgnoreCase);
        internal static object _lock = new object();

        [Configuration]
        internal static int Configuration_PackageSize = (512 + 1024) * 1024;

        [Configuration]
        internal static int Configuration_MemoryStreamCapacity = (1024 + 1024) * 1024;

        private static BlobPackage AcquireAndWriteHeader(string path, DateTime? date = null)
        {
            if (path == null) throw new ArgumentNullException();
            if (date != null && date.Value.Ticks == 0) date = null;
            path = Path.GetFullPath(path);
            var name = Path.GetFileName(path);
            var dirpath = Path.GetDirectoryName(path);
            lock (_lock)
            {
                var dir = GetDirectory(dirpath);
                BlobPackage pkg;
                if (dir.availablePackages.Count != 0)
                {
                    var idx = dir.availablePackages.Count - 1;
                    pkg = dir.availablePackages[idx];
                    dir.availablePackages.RemoveAt(idx);
                }
                else
                {
                    pkg = new BlobPackage() { fileName = Guid.NewGuid() + ".shaman-blobs", directory = dir };
                }
                dir.busyPackages.Add(pkg);
                pkg.WriteHeader(name, date);
                return pkg;
            }
        }

        private static char[] trimChars = new[] { '\\', '/' };
        




        // must hold lock
        private static PackageDirectory GetDirectory(string dirpath)
        {
            Debug.Assert(dirpath[0] == '/' || dirpath[1] == ':');
            dirpath = dirpath.TrimEnd(trimChars);
            PackageDirectory dir;
            directories.TryGetValue(dirpath, out dir);
            if (dir == null)
            {
                dir = new PackageDirectory();
                dir.path = dirpath;
                directories[dirpath] = dir;
                var indexFile = Path.Combine(dirpath, ".shaman-blob-index");
                if (File.Exists(indexFile))
                {
                    using (var stream = File.Open(indexFile, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read))
                    using (var br = new BinaryReader(stream, Encoding.UTF8))
                    {
                        dir.locations = new Dictionary<string, FileLocation>();
                        string package = null;
                        while (true)
                        {
                            var tag = stream.ReadByte();
                            if (tag == -1) break;
                            if (tag == 1)
                            {
                                package = br.ReadString();
                            }
                            else if (tag != 0) throw new InvalidDataException();

                            DateTime? date = null;
                            var name = br.ReadString();
                            int? length = null;
                            if (name.Length == 0)
                            {
                                uint len = br.ReadUInt32();
                                var hasDate = (len & (1 << 31)) != 0;
                                length = (int)(len & ~(1 << 31));
                                if (hasDate)
                                    date = new DateTime(br.ReadInt64(), DateTimeKind.Utc);
                                name = br.ReadString();
                            }
                            var offset = br.ReadInt32();
                            if (offset == -1)
                            {
                                dir.locations.Remove(name);
                            }
                            else
                            {

                                dir.locations[name] = new FileLocation()
                                {
                                    BlobName = name,
                                    DataStartOffset = offset,
                                    PackageFileName = package,
                                    Date = date,
                                    Length = length
                                };
                            }
                        }

                    }
                }
                else
                {
                    dir.locations = new Dictionary<string, FileLocation>();

                    if (
#if NET35
                        Directory.GetFiles(
#else
                        Directory.EnumerateFiles(
#endif
                        dirpath, "*.shaman-blobs"
                        ).Any())
                    {
                        IndexDirectory(dir, dirpath);
                    }
                }
            }
            return dir;
        }

        public static void FlushAll()
        {
            lock (_lock)
            {
                foreach (var dir in directories.Values)
                {
                    dir.Flush(false);
                }
            }
        }

        public static void CloseAll()
        {
            lock (_lock)
            {
                foreach (var dir in directories.Values)
                {
                    dir.Flush(true);
                }
                directories.Clear();
            }
        }

        public static void CloseAllForShutdown()
        {
            lock (_lock)
            {
                CloseAll();
                directories = null;
                _lock = null;
            }
        }


        public static void Delete(string path)
        {
            if (path == null) throw new ArgumentNullException();
            path = Path.GetFullPath(path);
            var blobName = Path.GetFileName(path);
            var dirpath = Path.GetDirectoryName(path);
            lock (_lock)
            {
                var dir = GetDirectory(dirpath);
                FileLocation location;
                dir.locations.TryGetValue(blobName, out location);
                if (location == null) return;
                dir.locationsToAdd.Add(new FileLocation()
                {
                    BlobName = blobName,
                    DataStartOffset = -1
                });
                dir.locations.Remove(blobName);
                using (var fs = location.Package?.ms == null ? (Stream)File.Open(Path.Combine(dir.path, location.PackageFileName), FileMode.Open, FileAccess.ReadWrite, FileShare.Delete | FileShare.ReadWrite) : null)
                {

                    var oldposition = fs == null ? location.Package.ms.Position : -1;
                    var stream = fs;
                    if (fs == null)
                    {
                        stream = location.Package.ms;
                    }
                    SeekToBeginOfBlobName(location, stream);
                    stream.Seek(-1, SeekOrigin.Current);
                    stream.WriteByte(1);

                    if (fs == null)
                        stream.Seek(oldposition, SeekOrigin.Begin);
                }


            }
            Debug.Assert(!Exists(path));
        }

        private static void SeekToBeginOfBlobName(FileLocation location, Stream stream)
        {
            var idx = 1;
            while (true)
            {
                stream.Seek(location.DataStartOffset - idx, SeekOrigin.Begin);
                var b = stream.ReadByte();
                if (b == 0 && idx == 1)
                {
                    idx += 8;
                    continue;
                }
                if (b == 0 || b == 1) break;
                idx++;
            }
        }

        public static string ReadAllText(string path)
        {
            using (var sr = new StreamReader(OpenRead(path), Encoding.UTF8, true))
            {
                return sr.ReadToEnd();
            }
        }

        public static int GetLength(string path)
        {
            using (var stream = OpenRead(path))
            {
                return (int)stream.Length;
            }
        }


#if NET35
        private static void InternalCopyTo(Stream source, Stream destination, int bufferSize)
        {
            byte[] array = new byte[bufferSize];
            int count;
            while ((count = source.Read(array, 0, array.Length)) != 0)
            {
                destination.Write(array, 0, count);
            }
        }

#endif

        public static void MigrateFolder(string folder)
        {
            if (folder == null) throw new ArgumentNullException();
            folder = Path.GetFullPath(folder);
#if NET35
            foreach (var file in Directory.GetFiles(folder))
#else
            foreach (var file in Directory.EnumerateFiles(folder))
#endif
            {
                var ext = Path.GetExtension(file);
                if (ext == ".shaman-blobs") continue;
                if (ext == ".shaman-blob-index") continue;
                if (BlobStore.Exists(file)) continue;
                Console.WriteLine(file);
                using (var fs = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read))
                {
                    using (var blob = OpenWrite(file))
                    {
#if NET35
                        InternalCopyTo(fs, blob, 81920);
#else
                        fs.CopyTo(blob);
#endif
                        blob.Commit();
                    }
                }
            }
            var dir = GetDirectory(folder);
            dir.Flush(true);
            Console.WriteLine("Migration done, deleting files...");
            foreach (var file in Directory
#if NET35
            .GetFiles(
#else
            .EnumerateFiles(
#endif
                folder))
            {
                var ext = Path.GetExtension(file);

                if (ext == ".shaman-blobs") continue;
                if (ext == ".shaman-blob-index") continue;
                File.Delete(file);
            }
            Console.WriteLine("Files deleted.");
        }

        public static void FlushDirectory(string path)
        {
            if (path == null) throw new ArgumentNullException();
            path = Path.GetFullPath(path);
            var dir = GetDirectory(path);
            dir.Flush(false);
        }
    }
}
