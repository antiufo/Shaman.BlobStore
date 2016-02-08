#if CORECLR

using System;
using System.IO;

namespace Shaman.Runtime
{
    internal static class ExtensionMethods
    {
        public static byte[] GetBuffer(this MemoryStream ms)
        {
            ArraySegment<byte> buffer;
            if(!ms.TryGetBuffer(out buffer)) throw new ArgumentException();
            return buffer.Array;
        }
      
    }
}

#endif