using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
#if !NET35
using System.Threading.Tasks;
#endif

namespace Shaman.Runtime
{
#if SHAMAN_BLOBSTORE
    internal
#else
    public
#endif
    class UnseekableStreamWrapper : Stream
    {
        public UnseekableStreamWrapper(Stream stream, long length)
        {
            this.inner = stream;
            this._length = length;
        }
        
        public UnseekableStreamWrapper(Stream stream)
        {
            this.inner = stream;
            this._length = -1;
        }
        
        private Stream inner;
        private long _length;
        public override bool CanRead
        {
            get
            {
                return inner.CanRead;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override long Length
        {
            get
            {
                if (_length == -1) throw new NotSupportedException();
                return _length;
            }
        }

        public override long Position
        {
            get
            {
                return _position;
            }

            set
            {
                Seek(value, SeekOrigin.Begin);
            }
        }
        private long _position;

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var v = inner.Read(buffer, offset, count);
            _position += v;
            return v;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            var pos =
                origin == SeekOrigin.Begin ? offset :
                origin == SeekOrigin.Current ? Position + offset :
                origin == SeekOrigin.End ? Length + offset :
                -1;
            if (pos < 0 || (_length != -1 && pos > _length)) throw new ArgumentException();
            if (pos == _position) return _position;
            if (pos < _position) throw new NotSupportedException("Cannot rewind stream.");
            var diff = pos - _position;
            var buffer = new byte[Math.Min(81920, diff)];
            while (true)
            {
                var read = Read(buffer, 0, (int)Math.Min((long)diff, buffer.Length));
                if (read == 0)
                {
                    if (_position == pos) return pos;
                    throw new EndOfStreamException();
                }
                diff -= read;
                if (diff == 0) return pos;
            }

        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

#if !NET35
        public async override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var k = await inner.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
            _position += k;
            return k;
        }
#endif

        public override int ReadTimeout
        {
            get
            {
                return inner.ReadTimeout;
            }

            set
            {
                inner.ReadTimeout = value;
            }
        }

#if !CORECLR
        public override void Close()
        {
            inner.Close();
        }
#endif

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
        }
        public override bool CanTimeout
        {
            get
            {
                return inner.CanTimeout;
            }
        }
        public override int ReadByte()
        {
            var k = inner.ReadByte();
            if (k != -1)
            {
                _position++;
            }
            return k;
        }

    }
}
