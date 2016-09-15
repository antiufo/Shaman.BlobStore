# Shaman.BlobStore
 Provides an almost drop-in replacement for `System.IO.File`, optimized for a very large number of small files.
 Data is batched in package files, and can be read sequentially.

 ## Usage
 ```csharp
 using Shaman.Runtime;

// Physical structure on file system:
// C:\\Example\\.shaman-blob-index   <- index, automatically regenerated if missing
// C:\\Example\\044cf7e7-d3a8-4499-b04d-0f5bab478e64.shaman-blobs  <- package #1
// C:\\Example\\37adb1a9-d365-48ab-bfb1-ff6c0fc7b552.shaman-blobs  <- package #2
// C:\\Example\\fc08265d-1a1c-4fc9-86b8-86475486491a.shaman-blobs  <- package #3

foreach (Blob b in BlobStore.EnumerateFiles("C:\\Example"))
{
    // reading thousands or millions of small blobs, stored sequentially in .shaman-blobs files
    using (Stream s = b.OpenRead())
    {
        // Process blob
    }
}

// Not actual files on the file system, they're located in one of the packages (*.shaman-blobs) in C:\Example
BlobStore.Delete("C:\\Example\\MyFile1.txt");
BlobStore.WriteAllText("C:\\Example\\MyFile2.txt", "content", Encoding.UTF8);
BlobStore.FlushDirectory("C:\\Example");

// Each package is flushed and saved as a file once it grows above 1.5 MB (by default)
// You can change the batch size (in KB) with:
BlobStore.SetConfigurationForDirectory(@"C:\\Example\\", 10 * 1024);
```