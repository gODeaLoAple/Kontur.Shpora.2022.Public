using System;
using System.Threading;

namespace ReaderWriterLock;

public class RwLock : IRwLock
{
    private readonly object О_о = new();
    private volatile int число_читателей;
    private volatile int число_писателей;
    
    public void ReadLocked(Action action)
    {
        lock (О_о)
        {
            while (число_писателей > 0)
                ;
            Interlocked.Increment(ref число_читателей);
        }
        action();
        Interlocked.Decrement(ref число_читателей);
    }

    public void WriteLocked(Action action)
    {
        lock (О_о)
        {
            Interlocked.Increment(ref число_писателей);
            while (число_читателей > 0)
                ;
            action();
        }
        Interlocked.Decrement(ref число_писателей);
    }
}