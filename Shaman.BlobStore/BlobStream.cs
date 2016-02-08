using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Shaman.Runtime
{
    public class BlobStream : Stream
    {
        private Stream readStream;
        private int length;
        private int remainingToRead;
        private BlobPackage package;
        private bool leaveOpen;
        private bool autocommit;

        internal BlobStream(Stream readStream, int length, bool leaveOpen)
        {
            this.readStream = readStream;
            this.length = length;
            this.remainingToRead = length;
            this.leaveOpen = leaveOpen;
        }
        internal BlobStream(BlobPackage pkg, bool autocommit)
        {
            this.package = pkg;
            this.autocommit = autocommit;
        }
        public override int ReadByte()
        {
            if (remainingToRead-- == 0) return -1;
            return readStream.ReadByte();
        }

        public override void WriteByte(byte value)
        {
            package.EnsureAdditionalCapacity(1).WriteByte(value);
        }
        public override bool CanRead
        {
            get
            {
                return readStream != null;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return CanRead;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return package != null;
            }
        }

        public override long Length
        {
            get
            {
                if (package != null) return package.ms.Length - package.startOfBlobData;
                return length;
            }
        }

        public override long Position
        {
            get
            {
                if (package != null) return Length;
                return length - remainingToRead;
            }

            set
            {
                throw new NotSupportedException();
            }
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (remainingToRead == 0) return 0;
            var r = readStream.Read(buffer, offset, Math.Min(remainingToRead, count));
            remainingToRead -= r;
            return r;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (readStream == null) throw new NotSupportedException();
            var newPosition = origin == SeekOrigin.Begin ? 0 : origin == SeekOrigin.Current ? Position : Length;
            newPosition += offset;
            if (newPosition < 0 || newPosition >= Length) throw new ArgumentOutOfRangeException();
            var diff = (int)(newPosition - Position);
            remainingToRead -= diff;
            readStream.Seek(diff, SeekOrigin.Current);
            return Position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            package.EnsureAdditionalCapacity(count).Write(buffer, offset, count);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (readStream != null)
                {
                    if (!leaveOpen)
                        readStream.Dispose();
                }
                else
                {
                    var pkg = Interlocked.Exchange(ref package, null);
                    if (pkg != null)
                    {
                        pkg.Release(autocommit);
                    }
                }
            }
        }


        public void Commit()
        {
            var pkg = Interlocked.Exchange(ref package, null);
            if (pkg != null)
            {
                pkg.Release(true);
            }
            else
            {
                throw new InvalidOperationException();
            }
        }
    }
}
