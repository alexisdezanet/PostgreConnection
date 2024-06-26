using System;

namespace PostgreConnection.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class DistinctPropertyAttribute : Attribute
    {
        public DistinctPropertyAttribute()
        {
        }
    }
}
